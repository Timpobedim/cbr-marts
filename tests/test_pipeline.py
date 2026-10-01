from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import httpx
from conftest import FIRST, FRIDAY, LAST, SATURDAY, payload

from cbr.pipeline import make_dag, quality
from cbr.source import Client, parse
from cbr.store import DONE, SKIPPED, Store


def test_month_is_loaded_without_weekend_duplicates(store: Store, client: Client) -> None:
    dag = make_dag(client)
    reports = dag.backfill(FIRST, LAST, store)

    assert all(report.ok for report in reports)
    days = {row[0] for row in store.query("SELECT DISTINCT rate_date FROM fact_rate")}
    assert all(day.weekday() < 5 for day in days), "в факте только рабочие дни"
    assert len(days) < len(reports), "выходные не добавляют новых дней"
    assert store.count("fact_rate") == len(days) * 6


def test_weekend_run_says_so(store: Store, client: Client) -> None:
    report = make_dag(client).run_for(SATURDAY, store)
    extract = report.results[0]
    assert extract.status == DONE
    assert "выходной" in extract.message


def test_second_run_of_the_same_day_does_nothing(store: Store, client: Client) -> None:
    dag = make_dag(client)
    dag.run_for(FRIDAY, store)
    again = dag.run_for(FRIDAY, store)
    assert [result.status for result in again.results] == [SKIPPED] * 4


def test_raw_answers_are_journalled(store: Store, client: Client) -> None:
    make_dag(client).run_for(SATURDAY, store)
    requested, actual, size = store.one("SELECT requested, actual, size_bytes FROM raw_answer")
    assert requested == SATURDAY
    assert actual == SATURDAY - timedelta(days=1)
    assert int(size) > 0


def test_missing_source_file_fails_the_day(store: Store, tmp_path: Path) -> None:
    empty = Client(
        cache=tmp_path / "пусто",
        client=httpx.Client(transport=httpx.MockTransport(_no_network)),
        retries=0,
        pause=0.0,
        sleep=lambda seconds: None,
    )
    report = make_dag(empty).run_for(date(2026, 2, 2), store)
    assert not report.ok
    assert report.results[0].status != DONE
    assert [result.status for result in report.results[1:]] == ["не запускалась"] * 3


def test_quality_notices_an_empty_warehouse(store: Store) -> None:
    assert quality(store) == ["в хранилище нет ни одного курса"]


def test_quality_notices_a_missing_currency(store: Store) -> None:
    rates = {"USD": (1, Decimal("84.0"), "Доллар США"), "EUR": (1, Decimal("97.0"), "Евро")}
    store.upsert_rates(parse(payload(FRIDAY, rates), FRIDAY))
    problems = quality(store)
    assert any("нет курсов: CNY" in problem for problem in problems)


def test_quality_notices_a_hole_in_history(store: Store) -> None:
    store.upsert_rates(parse(payload(date(2026, 2, 2)), date(2026, 2, 2)))
    store.upsert_rates(parse(payload(date(2026, 2, 20)), date(2026, 2, 20)))
    assert any("дыра" in problem for problem in quality(store))


def test_quality_notices_a_truncated_answer(store: Store) -> None:
    store.upsert_rates(parse(payload(date(2026, 2, 2)), date(2026, 2, 2)))
    short = {"USD": (1, Decimal("84.0"), "Доллар США")}
    store.upsert_rates(parse(payload(date(2026, 2, 3), short), date(2026, 2, 3)))
    assert any("против" in problem for problem in quality(store))


def _no_network(request: httpx.Request) -> httpx.Response:
    """Проверки не ходят в сеть.

    Раньше этот тест проходил по неверной причине: запрос падал на кириллице в User-Agent. После
    исправления заголовка он пошёл бы качать настоящие курсы — теперь сети в нём нет совсем.
    """
    raise httpx.ConnectError("в проверках сети нет")
