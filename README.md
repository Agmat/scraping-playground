# elec-scraping

Scrapes [GPI TGE](https://gpi.tge.pl/en/wit) Urgent Market Messages (UMM),
filtered to **Electricity**, into a local SQLite database.

## Requirements

Python ≥ 3.12 and [uv](https://docs.astral.sh/uv/). Dependencies (`scrapling`)
are installed on first run.

## Usage

```sh
uv run scrape.py                  # resume/continue the crawl
uv run scrape.py --max-pages 2    # smoke test: only crawl 2 list pages
uv run scrape.py --since-days 7   # stop once messages older than a week are reached
uv run scrape.py --export         # dump umm.json from the database
uv run scrape.py --export out.json --db other.sqlite
```

| Flag | Default | Meaning |
|---|---|---|
| `--db` | `umm.sqlite` | SQLite file to read/write |
| `--max-pages` | all | stop after N list pages |
| `--since-days` | none | stop once a message older than N days is reached |
| `--export [PATH]` | `umm.json` | export instead of crawling |

## How the crawl works

Two modes, chosen by the `backfill_done` flag in the `meta` table:

1. **Backfill** — walks list pages from `next_page` to the last page, storing
   every unseen `witid`. Progress is checkpointed after each page, so an
   interrupted run resumes where it stopped.
2. **Incremental** — once backfill finished, walks from page 1 and stops at the
   first page with no new messages.

Requests are spaced by `REQUEST_DELAY` (0.5s) and retried 3× with backoff. The
Electricity filter is a POST that sets a session cookie; if a list page comes
back unfiltered it is re-applied automatically. Per-message failures are stored
in the row's `error` column rather than aborting the crawl.

## Data

Table `umm`, one row per message:

| Column | Notes |
|---|---|
| `witid` | primary key, the site's internal id |
| `message_id`, `published_at`, `umm_type`, `title` | promoted out of `data` for querying |
| `data` | full parsed record as JSON, including the `intervals` list |
| `url` | detail page URL |
| `scraped_at` | UTC ISO timestamp |
| `error` | non-null if this message failed to parse/fetch |

Detail-page `dt` labels are mapped to snake_case keys via `LABELS` in
`scrape.py` (which also fixes the site's own spelling of
"Unavailibility"/"Unavailabillity"). Unknown labels fall back to a slug of the
label, so new fields land in `data` instead of being dropped.

Table `meta` holds crawl state: `next_page`, `total_pages`, `backfill_done`.

## Tests

Parsing is checked against saved HTML in `fixtures/`, no network:

```sh
uv run test_scrape.py
```
