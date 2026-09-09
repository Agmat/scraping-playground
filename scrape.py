#!/usr/bin/env python3
"""Scrape GPI TGE Urgent Market Messages (UMM) into SQLite.

Crawls unfiltered rather than applying the site's ELECTRICITY filter: the
filter is session-only state, but the site's shared nginx cache keys list
pages by URL and ignores the session cookie, so a filtered crawl can be
served a mix of filtered and unfiltered pages for the same URL with no way
to detect or recover from it. Unfiltered removes the ambiguity; the cost is
~49 non-electricity records out of 202k, which are still captured.

Usage:
    uv run scrape.py                  # resume/continue the crawl
    uv run scrape.py --max-pages 2    # smoke test: only crawl 2 list pages
    uv run scrape.py --since-days 7   # stop once messages older than a week are reached
    uv run scrape.py --export         # dump umm.json from the database
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sqlite3
import time
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
REQUEST_DELAY = 0.5

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
    conn.commit()


def known_witids(conn: sqlite3.Connection) -> set[int]:
    return {row[0] for row in conn.execute("SELECT witid FROM umm")}


def insert_record(conn: sqlite3.Connection, witid: int, url: str, record: dict | None, error: str | None) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO umm (witid, message_id, published_at, umm_type, title, data, url, scraped_at, error) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            witid,
            (record or {}).get("message_id"),
            (record or {}).get("published_at"),
            (record or {}).get("umm_type"),
            (record or {}).get("title"),
            json.dumps(record, ensure_ascii=False) if record else None,
            url,
            datetime.now(timezone.utc).isoformat(),
            error,
        ),
    )
    conn.commit()


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

def fetch(session, method: str, url: str, attempts: int = 3, **kw):
    """GET/POST with a delay plus outer retry+backoff on top of the session's
    own retries, so a transient 5xx on a *list* page doesn't kill the whole
    crawl (FetcherSession's built-in retries only cover connection failures,
    not HTTP error statuses)."""
    call = getattr(session, method)
    last_exc: Exception | None = None
    for attempt in range(1, attempts + 1):
        time.sleep(REQUEST_DELAY)
        try:
            r = call(url, **kw)
            if r.status != 200:
                raise RuntimeError(f"{method.upper()} {url} -> {r.status}")
            return r
        except Exception as exc:  # noqa: BLE001 - retry, then propagate
            last_exc = exc
            if attempt < attempts:
                backoff = 2 * attempt
                log.warning("attempt %d/%d failed (%s), retrying in %ds", attempt, attempts, exc, backoff)
                time.sleep(backoff)
    raise last_exc


def fetch_list_page(session, page_num: int):
    return fetch(session, "get", f"{LIST_URL}&_witshortlistportlet_WAR_witportlet_page={page_num}")


class CutoffReached(Exception):
    """Raised when a fetched record is older than the --since-days cutoff."""


def scrape_detail(session, conn: sqlite3.Connection, witid: int, cutoff: datetime | None) -> None:
    url = DETAIL_URL.format(witid=witid)
    try:
        r = fetch(session, "get", url)
        record = parse_detail(r)
        insert_record(conn, witid, url, record, None if record else "no details")
    except Exception as exc:  # noqa: BLE001 - keep crawling, log the failure
        log.error("witid %s failed: %s", witid, exc)
        insert_record(conn, witid, url, None, str(exc))
        return

    if cutoff is not None and record and record.get("published_at"):
        try:
            published = datetime.strptime(record["published_at"], "%Y-%m-%d %H:%M:%S")
        except ValueError:
            published = None
        if published is not None and published < cutoff:
            raise CutoffReached(f"witid {witid} published {record['published_at']} < cutoff {cutoff}")


def crawl(conn: sqlite3.Connection, max_pages: int | None, since_days: int | None) -> None:
    cutoff = datetime.now() - timedelta(days=since_days) if since_days is not None else None

    with FetcherSession(impersonate="chrome", timeout=30, retries=3, retry_delay=2) as session:
        backfill_done = get_meta(conn, "backfill_done") == "1"
        pages_done = 0

        try:
            if not backfill_done:
                next_page = int(get_meta(conn, "next_page", "1"))
                total_pages = int(get_meta(conn, "total_pages", "0")) or None
                page_num = next_page
                while True:
                    if max_pages is not None and pages_done >= max_pages:
                        break
                    r = fetch_list_page(session, page_num)
                    ids, tp = parse_list(r)
                    if tp:
                        total_pages = tp
                        set_meta(conn, "total_pages", str(total_pages))
                    known = known_witids(conn)
                    new_ids = [i for i in ids if i not in known]
                    for witid in new_ids:
                        scrape_detail(session, conn, witid, cutoff)
                    log.info("backfill page %s/%s: %d new, %d skipped", page_num, total_pages, len(new_ids), len(ids) - len(new_ids))
                    set_meta(conn, "next_page", str(page_num + 1))
                    pages_done += 1
                    if total_pages is not None and page_num >= total_pages:
                        set_meta(conn, "backfill_done", "1")
                        log.info("backfill complete")
                        break
                    page_num += 1
            else:
                page_num = 1
                while True:
                    if max_pages is not None and pages_done >= max_pages:
                        break
                    r = fetch_list_page(session, page_num)
                    ids, _ = parse_list(r)
                    known = known_witids(conn)
                    new_ids = [i for i in ids if i not in known]
                    if not new_ids:
                        log.info("incremental page %s: nothing new, stopping", page_num)
                        break
                    for witid in new_ids:
                        scrape_detail(session, conn, witid, cutoff)
                    log.info("incremental page %s: %d new", page_num, len(new_ids))
                    pages_done += 1
                    page_num += 1
        except CutoffReached as exc:
            # Deliberately do not mark backfill_done: next_page already points
            # past here, so a later full run picks up right where this left off.
            log.info("stopping: %s", exc)


def export(conn: sqlite3.Connection, path: str) -> None:
    rows = conn.execute(
        "SELECT data FROM umm WHERE data IS NOT NULL ORDER BY witid"
    ).fetchall()
    records = [json.loads(row[0]) for row in rows]
    with open(path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    log.info("exported %d records to %s", len(records), path)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="umm.sqlite")
    parser.add_argument("--max-pages", type=int, default=None, help="stop after N list pages (smoke test)")
    parser.add_argument("--since-days", type=int, default=None, help="stop once messages older than N days are reached")
    parser.add_argument("--export", nargs="?", const="umm.json", default=None, metavar="PATH")
    args = parser.parse_args()

    conn = open_db(args.db)
    if args.export is not None:
        export(conn, args.export)
        return
    crawl(conn, args.max_pages, args.since_days)


if __name__ == "__main__":
    main()
