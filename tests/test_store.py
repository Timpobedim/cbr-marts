from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

from conftest import FRIDAY, SATURDAY, payload

from cbr.source import parse
from cbr.store import DONE, FAILED, Store


def test_rates_are_written_once_per_day(store: Store) -> None:
    daily = parse(payload(FRIDAY), FRIDAY)
    assert store.upsert_rates(daily) == 4
    store.upsert_rates(daily)
    assert store.count("fact_rate") == 4, "повторная запись того же дня не должна множить строки"
    assert store.count("dim_currency") == 4


def test_weekend_answer_lands_on_the_business_day(store: Store) -> None:
    store.upsert_rates(parse(payload(FRIDAY), FRIDAY))
    store.upsert_rates(parse(payload(FRIDAY), SATURDAY))
    days = {row[0] for row in store.query("SELECT DISTINCT rate_date FROM fact_rate")}
    assert days == {FRIDAY}, "суббота не должна создавать отдельный день в факте"


def test_corrected_value_overwrites(store: Store) -> None:
    store.upsert_rates(parse(payload(FRIDAY), FRIDAY))
    fixed = {"USD": (1, Decimal("90.0000"), "Доллар США")}
    store.upsert_rates(parse(payload(FRIDAY, fixed), FRIDAY))
    value = store.one("SELECT value FROM fact_rate WHERE code = 'USD' AND rate_date = ?", [FRIDAY])[0]
    assert str(value) == "90.0000"


def test_answers_are_journalled(store: Store) -> None:
    daily = parse(payload(FRIDAY), SATURDAY)
    store.record_answer(daily, size=1024, content_hash="abc", path="сырые/2026-02-07.xml")
    store.record_answer(daily, size=2048, content_hash="abc", path="сырые/2026-02-07.xml")
    row = store.one("SELECT requested, actual, size_bytes FROM raw_answer")
    assert (row[0], row[1], int(row[2])) == (SATURDAY, FRIDAY, 2048)


def test_rates_can_be_read_by_period(store: Store) -> None:
    for day in (date(2026, 2, 2), date(2026, 2, 3), date(2026, 2, 4)):
        store.upsert_rates(parse(payload(day), day))
    rows = store.rates("USD", since=date(2026, 2, 3))
    assert [row.rate_date for row in rows] == [date(2026, 2, 3), date(2026, 2, 4)]
    assert store.period() == (date(2026, 2, 2), date(2026, 2, 4))


def test_journal_keeps_the_latest_status(store: Store) -> None:
    store.record_attempt(
        dag="курсы-цб", task="извлечь", run_date=FRIDAY, attempt=1, status=FAILED, message="сеть", seconds=0.1
    )
    store.record_attempt(
        dag="курсы-цб", task="извлечь", run_date=FRIDAY, attempt=2, status=DONE, message="ответ", seconds=0.2
    )
    assert store.task_status("курсы-цб", "извлечь", FRIDAY) == DONE
    assert store.task_status("курсы-цб", "загрузить", FRIDAY) is None
    assert store.done_days("курсы-цб", "извлечь") == {FRIDAY}
    assert [row.attempt for row in store.journal()] == [1, 2]


def test_folder_for_the_warehouse_is_created(tmp_path: Path) -> None:
    """Хранилище создаёт свою папку: «cbr run --warehouse данные/курсы.duckdb» должен работать сразу."""
    path = tmp_path / "новая-папка" / "курсы.duckdb"
    with Store(path) as warehouse:
        assert warehouse.count("fact_rate") == 0
    assert path.exists()
