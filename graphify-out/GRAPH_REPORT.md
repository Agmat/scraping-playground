# Graph Report - elec-scraping  (2026-09-09)

## Corpus Check
- 9 files · ~81,652 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 64 nodes · 119 edges · 10 communities (5 shown, 5 thin omitted)
- Extraction: 91% EXTRACTED · 9% INFERRED · 0% AMBIGUOUS · INFERRED: 11 edges (avg confidence: 0.89)
- Token cost: 0 input · 68,791 output

## Community Hubs (Navigation)
- Parsing & Table Routing
- Database & Crawl Orchestration
- Async Fetching & Retry
- GPI UMM Data Model
- Crawl Modes & Pagination
- Field Label Normalization
- Witid Identifier
- Slug/Key Helpers
- Project Root
- Request Delay / Backoff

## God Nodes (most connected - your core abstractions)
1. `commit_chunk()` - 10 edges
2. `crawl()` - 10 edges
3. `parse_detail()` - 7 edges
4. `retry_failed()` - 7 edges
5. `classify_record()` - 6 edges
6. `ensure_table()` - 6 edges
7. `all_record_tables()` - 5 edges
8. `parse_list()` - 5 edges
9. `scrape_detail()` - 5 edges
10. `gather_list_pages()` - 5 edges

## Surprising Connections (you probably didn't know these)
- `GPI TGE (source site)` --conceptually_related_to--> `UMM detail record (planned outage)`  [INFERRED]
  README.md → fixtures/detail.html
- `umm table` --shares_data_with--> `UMM detail record (planned outage)`  [INFERRED]
  README.md → fixtures/detail.html
- `test_classify_record_routes_other_and_unknown()` --calls--> `classify_record()`  [EXTRACTED]
  test_scrape.py → scrape.py
- `test_ensure_table_rejects_bad_year()` --calls--> `ensure_table()`  [EXTRACTED]
  test_scrape.py → scrape.py
- `test_parse_list()` --calls--> `parse_list()`  [EXTRACTED]
  test_scrape.py → scrape.py

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **UMM record schema across scraper touchpoints** — fixtures_detail_umm_detail_record, fixtures_detail_unplanned_umm_detail_record, fixtures_list_page_witlist, readme_umm_table [INFERRED 0.85]
- **Backfill/incremental crawl flow via witid and pagination** — readme_backfill_mode, readme_incremental_mode, fixtures_list_page_pagination, readme_witid [INFERRED 0.85]

## Communities (10 total, 5 thin omitted)

### Community 0 - "Parsing & Table Routing"
Cohesion: 0.19
Nodes (14): classify_record(), clean(), ensure_table(), parse_detail(), Which table a parsed record routes to: umm_<year>, umm_other, or umm_unknown., Create a record table if needed. Re-validates the name even though…, Selector, load() (+6 more)

### Community 1 - "Database & Crawl Orchestration"
Cohesion: 0.34
Nodes (14): Connection, datetime, all_record_tables(), commit_chunk(), crawl(), export(), get_meta(), _handle_signal() (+6 more)

### Community 2 - "Async Fetching & Retry"
Cohesion: 0.23
Nodes (12): failed_retry_ids(), fetch(), fetch_list_page(), gather_details(), gather_list_pages(), parse_list(), Witids that failed for a retryable (non-permanent) reason., Return (witids on this page, total_pages) from a list-page response. (+4 more)

### Community 3 - "GPI UMM Data Model"
Cohesion: 0.28
Nodes (9): UMM detail record (planned outage), Previous UMM reference link, UMM detail record (unplanned outage), WIT list page (UMM listing), Electricity filter (session-cookie POST), Per-row error column, GPI TGE (source site), Urgent Market Message (UMM) (+1 more)

### Community 4 - "Crawl Modes & Pagination"
Cohesion: 0.67
Nodes (4): List page pagination (Page N of M), Backfill crawl mode, Incremental crawl mode, meta table

## Knowledge Gaps
- **6 isolated node(s):** `elec-scraping`, `GPI TGE (source site)`, `witid (site internal id)`, `Type of Unavailibility field`, `Previous UMM reference link` (+1 more)
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 19 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **5 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `classify_record()` connect `Parsing & Table Routing` to `Database & Crawl Orchestration`?**
  _High betweenness centrality (0.038) - this node is a cross-community bridge._
- **Why does `ensure_table()` connect `Parsing & Table Routing` to `Database & Crawl Orchestration`?**
  _High betweenness centrality (0.038) - this node is a cross-community bridge._
- **Why does `commit_chunk()` connect `Database & Crawl Orchestration` to `Parsing & Table Routing`, `Async Fetching & Retry`?**
  _High betweenness centrality (0.034) - this node is a cross-community bridge._
- **What connects `elec-scraping`, `GPI TGE (source site)`, `witid (site internal id)` to the rest of the system?**
  _6 weakly-connected nodes found - possible documentation gaps or missing edges._