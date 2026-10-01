from __future__ import annotations

import pytest
from conftest import FIRST, LAST

from cbr import marts, report
from cbr.pipeline import make_dag
from cbr.source import Client
from cbr.store import Store


@pytest.fixture
def filled(store: Store, client: Client) -> Store:
    make_dag(client).backfill(FIRST, LAST, store)
    return store


def test_all_marts_are_built(filled: Store) -> None:
    sizes = marts.build(filled)
    assert set(sizes) == {"mart_rate_daily", "mart_month", "mart_change", "mart_cross"}
    assert all(rows > 0 for rows in sizes.values())


def test_change_matches_the_last_two_days(filled: Store) -> None:
    latest, previous = [
        float(str(row[0]))
        for row in filled.query("SELECT per_unit FROM fact_rate WHERE code = 'USD' ORDER BY rate_date DESC LIMIT 2")
    ]
    change = filled.one("SELECT change_day FROM mart_change WHERE code = 'USD'")[0]
    assert float(change) == pytest.approx((latest - previous) / previous, rel=1e-6)


def test_month_mart_splits_february_and_march(filled: Store) -> None:
    months = filled.query("SELECT year, month, days FROM mart_month WHERE code = 'USD' ORDER BY year, month")
    assert [(int(row[0]), int(row[1])) for row in months] == [(2026, 2), (2026, 3)]
    assert sum(int(row[2]) for row in months) == filled.count("fact_rate", "code = 'USD'")


def test_cross_rate_is_eur_divided_by_usd(filled: Store) -> None:
    day, eur_usd = filled.one("SELECT rate_date, eur_usd FROM mart_cross ORDER BY rate_date DESC LIMIT 1")
    eur = float(str(filled.one("SELECT per_unit FROM fact_rate WHERE code='EUR' AND rate_date=?", [day])[0]))
    usd = float(str(filled.one("SELECT per_unit FROM fact_rate WHERE code='USD' AND rate_date=?", [day])[0]))
    assert float(eur_usd) == pytest.approx(eur / usd, rel=1e-6)


def test_daily_mart_carries_the_currency_name(filled: Store) -> None:
    name = filled.one("SELECT name FROM mart_rate_daily WHERE code = 'USD' LIMIT 1")[0]
    assert name == "Доллар США"


def test_console_report_shows_changes(filled: Store) -> None:
    text = report.summary(filled, journal=filled.journal(limit=5))
    assert "Изменения:" in text
    assert "USD" in text
    assert "%" in text


def test_page_has_cards_and_a_chart(filled: Store) -> None:
    page = report.dashboard(filled)
    assert "<title>Курсы ЦБ: сводка</title>" in page
    assert "Доллар США" in page
    assert "<svg" in page
    assert "polyline" in page


def test_empty_warehouse_page_does_not_break(store: Store) -> None:
    assert "Курсов пока нет" in report.dashboard(store)
    assert "пока нет курсов" in report.summary(store)


def test_percent_and_rate_formatting() -> None:
    assert report.percent(0.0123) == "+1.23%"
    assert report.percent(-0.005) == "-0.50%"
    assert report.percent(None) == "—"
    assert report.rate(84.2569) == "84.2569"
