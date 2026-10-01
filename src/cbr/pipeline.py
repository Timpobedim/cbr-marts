"""Задачи конвейера и граф, который их связывает.

Четыре шага: забрать ответ, разложить его в факт, пересобрать витрины, проверить результат. Каждый
шаг — обычная функция; оркестратор добавляет к ним порядок, повторы и журнал.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Final

from cbr import marts
from cbr.flow import Context, Dag, Task
from cbr.source import Client, digest, parse
from cbr.store import Store

DAG_NAME: Final = "курсы-цб"
REQUIRED: Final = ("USD", "EUR", "CNY")
"""Без этих трёх день считается неполным: такого в ответе ЦБ не бывает, значит, что-то сломалось."""

MAX_GAP_DAYS: Final = 10
"""Новогодние каникулы в России доходят до десяти дней — порог поднят до них, иначе каждый январь
проверка поднимала бы ложную тревогу."""


def make_dag(client: Client) -> Dag:
    """Собирает граф задач вокруг конкретного клиента источника."""

    def extract(context: Context) -> str:
        payload = client.payload(context.day)
        daily = parse(payload, context.day)
        path = client.raw_path(context.day)
        context.store.record_answer(
            daily, size=len(payload), content_hash=digest(payload), path=str(path) if path else "—"
        )
        if daily.moved:
            return f"ответ за {daily.actual:%d.%m.%Y} — выходной, курсы предыдущего рабочего дня"
        return f"ответ за {daily.actual:%d.%m.%Y}, {len(daily.rates)} валют"

    def load(context: Context) -> str:
        # Сеть здесь уже не нужна: ответ лежит в сыром слое после первого шага.
        daily = parse(client.payload(context.day), context.day)
        written = context.store.upsert_rates(daily)
        return f"{written} валют записано на {daily.actual:%d.%m.%Y}"

    def build_marts(context: Context) -> str:
        sizes = marts.build(context.store)
        return ", ".join(f"{name} — {rows}" for name, rows in sizes.items())

    def check(context: Context) -> str:
        problems = quality(context.store)
        if problems:
            raise RuntimeError("; ".join(problems))
        return "проверки пройдены"

    return Dag(
        DAG_NAME,
        [
            Task("извлечь", extract, retries=2, retry_delay=2.0),
            Task("загрузить", load, depends_on=("извлечь",), retries=1, retry_delay=1.0),
            Task("витрины", build_marts, depends_on=("загрузить",), retries=0),
            Task("проверить", check, depends_on=("витрины",), retries=0),
        ],
    )


def quality(store: Store) -> list[str]:
    """Четыре проверки по собранному хранилищу. Пустой список — всё в порядке."""
    problems: list[str] = []
    first, last = store.period()
    if last is None or first is None:
        return ["в хранилище нет ни одного курса"]

    missing = [
        code
        for code in REQUIRED
        if not store.query("SELECT 1 FROM fact_rate WHERE rate_date = ? AND code = ?", [last, code])
    ]
    if missing:
        problems.append(f"за {last:%d.%m.%Y} нет курсов: {', '.join(missing)}")

    wrong = store.count("fact_rate", "per_unit <= 0 OR nominal <= 0")
    if wrong:
        problems.append(f"неположительных курсов: {wrong}")

    gap = _longest_gap(store, first, last)
    if gap > MAX_GAP_DAYS:
        problems.append(f"в истории дыра в {gap} дней подряд")

    shrink = _shrink(store, last)
    if shrink:
        problems.append(shrink)
    return problems


def _longest_gap(store: Store, first: date, last: date) -> int:
    days = {row[0] for row in store.query("SELECT DISTINCT rate_date FROM fact_rate")}
    longest = 0
    current = 0
    day = first
    while day <= last:
        if day in days:
            current = 0
        else:
            current += 1
            longest = max(longest, current)
        day += timedelta(days=1)
    return longest


def _shrink(store: Store, last: date) -> str:
    """Резкое падение числа валют — признак обрезанного ответа, а не новостей с рынка."""
    rows = store.query("SELECT rate_date, count(*) FROM fact_rate GROUP BY rate_date ORDER BY rate_date DESC LIMIT 2")
    if len(rows) < 2:
        return ""
    today_count = int(rows[0][1])
    before_count = int(rows[1][1])
    if before_count and today_count < before_count * 0.8:
        return f"за {last:%d.%m.%Y} валют {today_count} против {before_count} накануне"
    return ""
