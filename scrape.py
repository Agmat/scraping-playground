#!/usr/bin/env python3
"""Scrape GPI TGE Urgent Market Messages (UMM) into SQLite.

Crawls unfiltered rather than applying the site's ELECTRICITY filter: the
filter is session-only state, but the site's shared nginx cache keys list
pages by URL and ignores the session cookie, so a filtered crawl can be
served a mix of filtered and unfiltered pages for the same URL with no way
to detect or recover from it. Unfiltered removes the ambiguity; the cost is
~49 non-electricity records out of 202k, which are still captured.

Fetches concurrently in chunks of list pages; each chunk commits atomically
(rows + resume cursor together), so an interruption at any point costs at
most one chunk and never leaves a half-written page behind.

Usage:
    uv run scrape.py                        # resume/continue the crawl
    uv run scrape.py --max-pages 2          # smoke test: only crawl 2 list pages
    uv run scrape.py --since-days 7         # stop once messages older than a week are reached
    uv run scrape.py --concurrency 10       # more/fewer requests in flight
    uv run scrape.py --export               # dump umm.json from the database
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
import signal
import sqlite3
from datetime import datetime, timedelta, timezone

from scrapling.fetchers import FetcherSession

BASE = "https://gpi.tge.pl/en/wit"
# lifecycle=0 (rather than =1) skips the redirect/cookie dance and is not
# Disallow'd by robots.txt, unlike lifecycle=1.
LIST_URL = (
    f"{BASE}?p_p_id=witshortlistportlet_WAR_witportlet&p_p_lifecycle=0"
    "&p_p_state=normal&p_p_mode=view&p_p_col_id=column-1&p_p_col_count=1"
    "&_witshortlistportlet_WAR_witportlet_action=filter-wit"
)
DETAIL_URL = (
    f"{BASE}?p_p_id=witdisplayportlet_WAR_witportlet&p_p_lifecycle=0"
    "&_witdisplayportlet_WAR_witportlet_action=show-wit"
    "&_witdisplayportlet_WAR_witportlet_implicitModel=true"
    "&_witdisplayportlet_WAR_witportlet_witid={witid}"
)

# dt label -> snake_case key, with the site's own typos fixed.
LABELS = {
    "Message ID": "message_id",
    "Event Status": "event_status",
    "Type of Unavailibility": "type_of_unavailability",
    "Type of Event": "type_of_event",
    "Publication date/time": "published_at",
    "Event Start": "event_start",
    "Event Stop": "event_stop",
    "Unit of Measurement": "unit_of_measurement",
    "Installed Capacity": "installed_capacity",
    "Reason of the Unavailabillity": "reason",
    "Remarks": "remarks",
    "Fuel Type": "fuel_type",
    "Bidding Zone": "bidding_zone",
    "Affected Asset or Unit": "affected_asset",
    "Affected Asset or Unit EIC Code": "affected_asset_eic_code",
    "Market Participant": "market_participant",
    "Market Participant Code": "market_participant_code",
    "Source": "source",
    "Interval start": "interval_start",
    "Interval end": "interval_end",
    "Unavailable Capacity": "unavailable_capacity",
    "Available Capacity": "available_capacity",
}
INTERVAL_KEYS = {"interval_start", "interval_end", "unavailable_capacity", "available_capacity"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("scrape")

STOP = False


def _handle_signal(signum, frame) -> None:
    global STOP
    log.info("signal %s received, stopping after the current chunk", signum)
    STOP = True


def slugify(label: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", label.strip().lower()).strip("_")


def label_to_key(label: str) -> str:
    return LABELS.get(label, slugify(label))


def clean(text: str | None) -> str | None:
    if text is None:
        return None
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


# --- DB -----------------------------------------------------------------

def open_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute(
        """CREATE TABLE IF NOT EXISTS umm (
            witid INTEGER PRIMARY KEY,
            message_id TEXT,
            published_at TEXT,
            umm_type TEXT,
            title TEXT,
            data TEXT,
            url TEXT,
            scraped_at TEXT,
            error TEXT
        )"""
    )
    conn.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)")
    conn.commit()
    return conn


def get_meta(conn: sqlite3.Connection, key: str, default: str | None = None) -> str | None:
    row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    return row[0] if row else default


def set_meta(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)", (key, value))


def known_witids(conn: sqlite3.Connection) -> set[int]:
    """Witids that don't need fetching again: successes, plus permanent
    failures (a witid that genuinely has no detail page). Transient
    failures (network errors, 5xx) are deliberately excluded so they get
    retried rather than silently treated as done forever."""
    return {
        row[0]
        for row in conn.execute("SELECT witid FROM umm WHERE error IS NULL OR error = 'no details'")
    }


def failed_retry_ids(conn: sqlite3.Connection) -> list[int]:
    return [
        row[0]
        for row in conn.execute("SELECT witid FROM umm WHERE error IS NOT NULL AND error <> 'no details'")
    ]


def commit_chunk(
    conn: sqlite3.Connection,
    results: list[tuple[int, str, dict | None, str | None]],
    chunk_end_page: int | None,
    cutoff: datetime | None,
) -> bool:
    """Insert a chunk's worth of scrape results in one transaction.

    chunk_end_page: next_page value to persist, or None to leave it alone
        (incremental mode never tracks it; backfill leaves it alone when the
        cutoff was hit, so a later full run resumes at the same page and
        keeps going past where this run chose to stop).
    Returns True if any record in this chunk was older than the cutoff.
    """
    rows = []
    cutoff_hit = False
    now = datetime.now(timezone.utc).isoformat()

    for witid, url, record, error in results:
        rows.append((
            witid,
            (record or {}).get("message_id"),
            (record or {}).get("published_at"),
            (record or {}).get("umm_type"),
            (record or {}).get("title"),
            json.dumps(record, ensure_ascii=False) if record else None,
            url,
            now,
            error,
        ))
        if cutoff is not None and record and record.get("published_at"):
            try:
                published = datetime.strptime(record["published_at"], "%Y-%m-%d %H:%M:%S")
            except ValueError:
                published = None
            if published is not None and published < cutoff:
                cutoff_hit = True

    # OR REPLACE (not IGNORE): a witid retried out of the failed set must be
    # able to overwrite its old error row once it succeeds.
    conn.executemany(
        "INSERT OR REPLACE INTO umm (witid, message_id, published_at, umm_type, title, data, url, scraped_at, error) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    if chunk_end_page is not None and not cutoff_hit:
        set_meta(conn, "next_page", str(chunk_end_page))
    conn.commit()
    return cutoff_hit


# --- parsing --------------------------------------------------------------

def parse_list(page) -> tuple[list[int], int | None]:
    """Return (witids on this page, total_pages) from a list-page response."""
    hrefs = page.css("div.secondRow a::attr(href)").getall()
    ids = []
    for href in hrefs:
        m = re.search(r"_witdisplayportlet_WAR_witportlet_witid=(\d+)", href)
        if m:
            ids.append(int(m.group(1)))
    total_pages = None
    m = re.search(r'name="[^"]*_page"\s+value="\d+">\s*of\s+(\d+)', page.html_content)
    if m:
        total_pages = int(m.group(1))
    return ids, total_pages


def parse_detail(page) -> dict | None:
    items = page.css("ul.witList > li")
    if not items:
        return None

    record: dict = {"intervals": []}
    current_interval: dict | None = None

    for li in items:
        dt = clean(li.css("dt::text").get())
        dd = clean(" ".join(li.css("dd::text").getall()))
        if dt is None:
            continue
        key = label_to_key(dt)
        if key == "interval_start":
            current_interval = {}
            record["intervals"].append(current_interval)
        if key in INTERVAL_KEYS and current_interval is not None:
            current_interval[key] = dd
        else:
            record[key] = dd

    record["title"] = clean(" ".join(page.css("h3.blue ::text").getall()))
    record["umm_type"] = clean(page.css("div.witType::text").get())
    return record


# --- crawl ------------------------------------------------------------

async def fetch(session, method: str, url: str, attempts: int = 3, **kw):
    """GET with an outer retry+backoff on top of the session's own retries,
    so a transient 5xx doesn't kill the whole crawl (FetcherSession's retries
    only cover connection failures, not HTTP error statuses)."""
    call = getattr(session, method)
    last_exc: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            r = await call(url, **kw)
            if r.status != 200:
                raise RuntimeError(f"{method.upper()} {url} -> {r.status}")
            return r
        except Exception as exc:  # noqa: BLE001 - retry, then propagate
            last_exc = exc
            if attempt < attempts:
                backoff = 2 * attempt
                log.warning("attempt %d/%d failed (%s), retrying in %ds", attempt, attempts, exc, backoff)
                await asyncio.sleep(backoff)
    raise last_exc


async def fetch_list_page(session, sem: asyncio.Semaphore, page_num: int):
    async with sem:
        return await fetch(session, "get", f"{LIST_URL}&_witshortlistportlet_WAR_witportlet_page={page_num}")


async def scrape_detail(session, sem: asyncio.Semaphore, witid: int) -> tuple[int, str, dict | None, str | None]:
    url = DETAIL_URL.format(witid=witid)
    async with sem:
        try:
            r = await fetch(session, "get", url)
            record = parse_detail(r)
        except Exception as exc:  # noqa: BLE001 - one bad witid must not kill the crawl
            log.error("witid %s failed: %s", witid, exc)
            return witid, url, None, str(exc)
    return witid, url, record, (None if record else "no details")


async def gather_list_pages(session, sem: asyncio.Semaphore, pages: list[int]) -> tuple[list[int], int | None]:
    results = await asyncio.gather(
        *(fetch_list_page(session, sem, p) for p in pages), return_exceptions=True
    )
    all_ids: list[int] = []
    total_pages = None
    for p, res in zip(pages, results):
        if isinstance(res, Exception):
            log.error("list page %s failed: %s", p, res)
            continue
        ids, tp = parse_list(res)
        if tp:
            total_pages = tp
        all_ids.extend(ids)
    return all_ids, total_pages


async def gather_details(session, sem: asyncio.Semaphore, witids: list[int]):
    results = await asyncio.gather(
        *(scrape_detail(session, sem, w) for w in witids), return_exceptions=True
    )
    out = []
    for w, res in zip(witids, results):
        if isinstance(res, Exception):
            log.error("witid %s failed unexpectedly: %s", w, res)
            out.append((w, DETAIL_URL.format(witid=w), None, str(res)))
        else:
            out.append(res)
    return out


async def retry_failed(session, sem: asyncio.Semaphore, conn: sqlite3.Connection, concurrency: int, known: set[int]) -> None:
    retry_ids = failed_retry_ids(conn)
    if not retry_ids:
        return
    log.info("retrying %d previously failed witid(s)", len(retry_ids))
    for i in range(0, len(retry_ids), concurrency):
        if STOP:
            return
        batch = retry_ids[i : i + concurrency]
        results = await gather_details(session, sem, batch)
        commit_chunk(conn, results, chunk_end_page=None, cutoff=None)
        known.update(w for w, _, r, _ in results if r is not None)


async def crawl(conn: sqlite3.Connection, max_pages: int | None, since_days: int | None, concurrency: int) -> None:
    cutoff = datetime.now() - timedelta(days=since_days) if since_days is not None else None
    sem = asyncio.Semaphore(concurrency)
    chunk_pages = concurrency

    async with FetcherSession(impersonate="chrome", timeout=30, retries=3, retry_delay=2) as session:
        known = known_witids(conn)
        await retry_failed(session, sem, conn, concurrency, known)
        if STOP:
            return

        backfill_done = get_meta(conn, "backfill_done") == "1"
        pages_done = 0

        if not backfill_done:
            next_page = int(get_meta(conn, "next_page", "1"))
            total_pages = int(get_meta(conn, "total_pages", "0")) or None
            page_num = next_page
            log.info("resuming backfill at page %s of %s", page_num, total_pages or "?")

            while not STOP:
                if max_pages is not None and pages_done >= max_pages:
                    break
                pages_this_chunk = chunk_pages
                if max_pages is not None:
                    pages_this_chunk = min(pages_this_chunk, max_pages - pages_done)
                if total_pages is not None:
                    pages_this_chunk = min(pages_this_chunk, total_pages - page_num + 1)
                if pages_this_chunk <= 0:
                    break

                pages = list(range(page_num, page_num + pages_this_chunk))
                all_ids, tp = await gather_list_pages(session, sem, pages)
                if tp:
                    total_pages = tp
                    set_meta(conn, "total_pages", str(total_pages))
                    conn.commit()

                new_ids = [i for i in all_ids if i not in known]
                results = await gather_details(session, sem, new_ids)
                cutoff_hit = commit_chunk(
                    conn, results, chunk_end_page=page_num + pages_this_chunk, cutoff=cutoff
                )
                known.update(w for w, _, r, _ in results if r is not None)

                log.info(
                    "backfill pages %s-%s/%s: %d new, %d skipped",
                    pages[0], pages[-1], total_pages, len(new_ids), len(all_ids) - len(new_ids),
                )

                if cutoff_hit:
                    log.info("cutoff reached, stopping (next_page left at %s for a later full run)", page_num)
                    return

                pages_done += pages_this_chunk
                page_num += pages_this_chunk

                if total_pages is not None and page_num > total_pages:
                    set_meta(conn, "backfill_done", "1")
                    conn.commit()
                    log.info("backfill complete")
                    break
        else:
            page_num = 1
            while not STOP:
                if max_pages is not None and pages_done >= max_pages:
                    break
                pages = list(range(page_num, page_num + chunk_pages))
                all_ids, _ = await gather_list_pages(session, sem, pages)
                new_ids = [i for i in all_ids if i not in known]
                if not new_ids:
                    log.info("incremental pages %s-%s: nothing new, stopping", pages[0], pages[-1])
                    break
                results = await gather_details(session, sem, new_ids)
                commit_chunk(conn, results, chunk_end_page=None, cutoff=cutoff)
                known.update(w for w, _, r, _ in results if r is not None)
                log.info("incremental pages %s-%s: %d new", pages[0], pages[-1], len(new_ids))
                pages_done += chunk_pages
                page_num += chunk_pages


def export(conn: sqlite3.Connection, path: str) -> None:
    rows = conn.execute(
        "SELECT data FROM umm WHERE data IS NOT NULL ORDER BY witid"
    ).fetchall()
    records = [json.loads(row[0]) for row in rows]
    with open(path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    log.info("exported %d records to %s", len(records), path)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", default="umm.sqlite")
    parser.add_argument("--max-pages", type=int, default=None, help="stop after N list pages (smoke test)")
    parser.add_argument("--since-days", type=int, default=None, help="stop once messages older than N days are reached")
    parser.add_argument("--concurrency", type=int, default=5, help="max concurrent requests (default 5)")
    parser.add_argument("--export", nargs="?", const="umm.json", default=None, metavar="PATH")
    args = parser.parse_args()

    conn = open_db(args.db)
    if args.export is not None:
        export(conn, args.export)
        return

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)
    asyncio.run(crawl(conn, args.max_pages, args.since_days, args.concurrency))


if __name__ == "__main__":
    main()
