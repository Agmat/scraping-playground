"""Assert-based self-check for scrape.py's parsing, against saved fixtures."""
from scrapling.parser import Selector

from scrape import parse_detail, parse_list, filter_is_electricity


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


def test_filter_is_electricity() -> None:
    assert filter_is_electricity(load("fixtures/list_page.html")) is True
    assert filter_is_electricity(load("fixtures/detail.html")) is False


if __name__ == "__main__":
    test_parse_detail()
    test_parse_list()
    test_filter_is_electricity()
    print("all tests passed")
