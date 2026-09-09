"""Assert-based self-check for scrape.py's parsing and routing, against saved fixtures."""
from scrapling.parser import Selector

from scrape import (
    DETAIL_URL,
    LIST_URL,
    classify_record,
    ensure_table,
    parse_detail,
    parse_list,
)


def load(path: str) -> Selector:
    return Selector(open(path, encoding="utf-8").read())


def test_parse_detail() -> None:
    record = parse_detail(load("fixtures/detail.html"))
    assert record is not None
    assert record["message_id"] == "PGIEK_B05_E_090926_134102_001"
    assert record["event_status"] == "Active"
    assert record["type_of_unavailability"] == "Planned"
    assert record["fuel_type"] == "Fossil Hard coal"
    assert record["affected_asset"] == "Rybnik B5"
    assert record["affected_asset_eic_code"] == "19W000000000177S"
    assert record["umm_type"] == "UMM Type: Generation"
    assert record["title"] == "Active - Planned - Rybnik B5"
    assert len(record["intervals"]) == 1
    interval = record["intervals"][0]
    assert interval["interval_start"] == "2026-09-01 00:00:00"
    assert interval["interval_end"] == "2060-12-31 00:00:00"
    assert interval["unavailable_capacity"] == "209"
    assert interval["available_capacity"] == "0"
    # top-level record must not be polluted by interval-only keys
    assert "interval_start" not in record


def test_parse_list() -> None:
    ids, total_pages = parse_list(load("fixtures/list_page.html"))
    assert len(ids) == 8
    assert all(isinstance(i, int) for i in ids)
    assert total_pages == 25287


def test_urls_use_lifecycle_0() -> None:
    # p_p_lifecycle=1 is Disallow'd by robots.txt; lifecycle=0 also skips the
    # session cookie / redirect that the disallowed form requires.
    assert "p_p_lifecycle=0" in DETAIL_URL
    assert "p_p_lifecycle=0" in LIST_URL
    assert "p_p_lifecycle=1" not in DETAIL_URL
    assert "p_p_lifecycle=1" not in LIST_URL


def test_classify_record_routes_by_year() -> None:
    record = parse_detail(load("fixtures/detail.html"))
    assert record["published_at"].startswith("2026-")
    assert classify_record(record) == "umm_2026"


def test_classify_record_routes_other_and_unknown() -> None:
    assert classify_record({"umm_type": None, "published_at": "2026-01-01 00:00:00"}) == "umm_other"
    assert classify_record({"umm_type": "UMM Type: Generation", "published_at": None}) == "umm_unknown"
    assert classify_record({"umm_type": "UMM Type: Generation", "published_at": "not-a-date"}) == "umm_unknown"


def test_ensure_table_rejects_bad_year() -> None:
    import sqlite3

    conn = sqlite3.connect(":memory:")
    ensure_table(conn, "umm_2026")  # fine
    for bad in ["umm_2026; DROP TABLE meta;--", "umm_abcd", "umm_20266", "meta"]:
        try:
            ensure_table(conn, bad)
        except ValueError:
            continue
        raise AssertionError(f"expected ensure_table to reject {bad!r}")


if __name__ == "__main__":
    test_parse_detail()
    test_parse_list()
    test_urls_use_lifecycle_0()
    test_classify_record_routes_by_year()
    test_classify_record_routes_other_and_unknown()
    test_ensure_table_rejects_bad_year()
    print("all tests passed")
