"""Отчёт: таблицы в консоль и самодостаточная HTML-страница вместо Superset."""

from __future__ import annotations

import html
from collections.abc import Sequence
from datetime import date

from cbr.marts import TRACKED
from cbr.store import JournalRow, Store


def table(headers: Sequence[str], rows: Sequence[Sequence[str]], align: str = "") -> str:
    columns = len(headers)
    widths = [len(header) for header in headers]
    for row in rows:
        for index in range(columns):
            widths[index] = max(widths[index], len(row[index]))
    rules = (align + "l" * columns)[:columns]

    def render(cells: Sequence[str]) -> str:
        parts = [
            cells[index].rjust(widths[index]) if rules[index] == "r" else cells[index].ljust(widths[index])
            for index in range(columns)
        ]
        return "  ".join(parts).rstrip()

    return "\n".join([render(headers), *(render(row) for row in rows)])


def percent(value: object) -> str:
    if value is None:
        return "—"
    number = float(str(value)) * 100
    return f"{number:+.2f}%"


def rate(value: object) -> str:
    return "—" if value is None else f"{float(str(value)):.4f}"


def changes_table(store: Store) -> str:
    rows = store.query(
        "SELECT code, name, per_unit, change_day, change_week, change_month FROM mart_change ORDER BY code"
    )
    body = [
        [str(row[0]), str(row[1]), rate(row[2]), percent(row[3]), percent(row[4]), percent(row[5])]
        for row in rows
        if str(row[0]) in TRACKED
    ]
    return table(["Код", "Валюта", "Курс, ₽", "За день", "За неделю", "За месяц"], body, align="ll" + "r" * 4)


def months_table(store: Store, code: str = "USD") -> str:
    rows = store.query(
        "SELECT year, month, days, avg_rate, min_rate, max_rate, spread FROM mart_month WHERE code = ? "
        "ORDER BY year, month",
        [code],
    )
    body = [
        [
            f"{int(row[0])}",
            f"{int(row[1]):02d}",
            str(int(row[2])),
            rate(row[3]),
            rate(row[4]),
            rate(row[5]),
            percent(row[6]),
        ]
        for row in rows
    ]
    return table(["Год", "Месяц", "Дней", "Средний", "Минимум", "Максимум", "Размах"], body, align="ll" + "r" * 5)


def journal_table(rows: Sequence[JournalRow]) -> str:
    body = [
        [
            f"{row.run_date:%d.%m}",
            row.task,
            str(row.attempt),
            row.status,
            f"{row.seconds:.2f}",
            row.message[:60],
        ]
        for row in rows
    ]
    return table(["День", "Задача", "Попытка", "Итог", "Секунд", "Что вышло"], body, align="lll" + "lrl")


def summary(store: Store, *, journal: Sequence[JournalRow] = ()) -> str:
    first, last = store.period()
    if first is None or last is None:
        return "В хранилище пока нет курсов: запустите «cbr run» или «cbr backfill»."
    days = store.count("(SELECT DISTINCT rate_date FROM fact_rate)")
    parts = [
        f"Курсы с {first:%d.%m.%Y} по {last:%d.%m.%Y}: {days} рабочих дней, "
        f"{store.count('dim_currency')} валют, {store.count('fact_rate')} значений.",
        "",
        "Изменения:",
        changes_table(store),
        "",
        "Доллар по месяцам:",
        months_table(store, "USD"),
    ]
    if journal:
        parts += ["", "Последние запуски:", journal_table(journal)]
    return "\n".join(parts)


# ── страница ────────────────────────────────────────────────────────────────────────────────────

STYLE = """
:root { color-scheme: light dark; --ink:#151a21; --ink-2:#5c6773; --line:#e0e4e9; --bg:#f6f7f9;
        --card:#ffffff; --up:#b3261e; --down:#1f6b3a; }
@media (prefers-color-scheme: dark) {
  :root { --ink:#e9edf2; --ink-2:#98a2ad; --line:#2a3138; --bg:#11151a; --card:#191e24;
          --up:#ff8a80; --down:#8fd6a8; }
}
* { box-sizing: border-box; }
body { margin:0; padding:24px 16px 48px; background:var(--bg); color:var(--ink);
       font:15px/1.5 "Segoe UI", Roboto, Arial, sans-serif; }
main { max-width: 960px; margin: 0 auto; }
h1 { font-size:1.5rem; margin:0 0 4px; }
.sub { color:var(--ink-2); margin:0 0 24px; }
.cards { display:grid; grid-template-columns: repeat(auto-fill, minmax(220px, 1fr)); gap:12px; margin-bottom:20px; }
.card { background:var(--card); border:1px solid var(--line); border-radius:10px; padding:14px; }
.code { font-size:.8rem; color:var(--ink-2); letter-spacing:.08em; }
.value { font-size:1.5rem; font-variant-numeric: tabular-nums; margin:2px 0 6px; }
.delta { font-size:.85rem; font-variant-numeric: tabular-nums; }
.up { color:var(--up); } .down { color:var(--down); }
svg { display:block; width:100%; height:40px; margin-top:8px; }
table { border-collapse:collapse; width:100%; font-size:.9rem; background:var(--card);
        border:1px solid var(--line); border-radius:10px; overflow:hidden; }
th, td { text-align:right; padding:7px 10px; border-bottom:1px solid var(--line); font-variant-numeric: tabular-nums; }
th:first-child, td:first-child, th:nth-child(2), td:nth-child(2) { text-align:left; }
h2 { font-size:1.05rem; margin:24px 0 10px; }
"""


def dashboard(store: Store, *, generated_on: date | None = None) -> str:
    """Страница со сводкой: карточка на валюту, график за период и таблица по месяцам."""
    first, last = store.period()
    if first is None or last is None:
        body = "<p>Курсов пока нет.</p>"
        stamp = "—"
    else:
        body = _cards(store) + _months_html(store)
        stamp = f"{first:%d.%m.%Y} — {last:%d.%m.%Y}"
    generated = generated_on or last or date.today()  # noqa: DTZ011 — только для подписи на странице
    return (
        '<!doctype html>\n<html lang="ru">\n<head>\n<meta charset="utf-8">\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1">\n'
        "<title>Курсы ЦБ: сводка</title>\n"
        f"<style>{STYLE}</style>\n</head>\n<body>\n<main>\n"
        f"<h1>Курсы Банка России</h1>\n"
        f'<p class="sub">Период: {stamp}. Собрано {generated:%d.%m.%Y}.</p>\n'
        f"{body}\n</main>\n</body>\n</html>\n"
    )


def _cards(store: Store) -> str:
    rows = store.query("SELECT code, name, per_unit, change_day, change_week FROM mart_change ORDER BY code")
    cards = []
    for code, name, per_unit, change_day, change_week in rows:
        if str(code) not in TRACKED:
            continue
        series = [
            float(str(item[0]))
            for item in store.query("SELECT per_unit FROM mart_rate_daily WHERE code = ? ORDER BY rate_date", [code])
        ]
        direction = "up" if (change_day or 0) > 0 else "down"
        cards.append(
            f'<div class="card"><div class="code">{html.escape(str(code))} · {html.escape(str(name))}</div>'
            f'<div class="value">{rate(per_unit)} ₽</div>'
            f'<div class="delta {direction}">день {percent(change_day)} · неделя {percent(change_week)}</div>'
            f"{_spark(series)}</div>"
        )
    return f'<div class="cards">{"".join(cards)}</div>'


def _spark(values: Sequence[float], width: int = 200, height: int = 40) -> str:
    if len(values) < 2:
        return '<svg role="img" aria-label="мало данных"></svg>'
    low, high = min(values), max(values)
    span = (high - low) or 1.0
    step = width / (len(values) - 1)
    points = " ".join(
        f"{index * step:.1f},{height - (value - low) / span * (height - 4) - 2:.1f}"
        for index, value in enumerate(values)
    )
    return (
        f'<svg viewBox="0 0 {width} {height}" preserveAspectRatio="none" role="img" aria-label="динамика курса">'
        f'<polyline points="{points}" fill="none" stroke="currentColor" stroke-width="1.5" opacity="0.75"/></svg>'
    )


def _months_html(store: Store) -> str:
    rows = store.query(
        "SELECT code, year, month, days, avg_rate, min_rate, max_rate FROM mart_month "
        "WHERE code IN ('USD', 'EUR', 'CNY') ORDER BY code, year, month"
    )
    body = "".join(
        f"<tr><td>{html.escape(str(row[0]))}</td><td>{int(row[1])}-{int(row[2]):02d}</td>"
        f"<td>{int(row[3])}</td><td>{rate(row[4])}</td><td>{rate(row[5])}</td><td>{rate(row[6])}</td></tr>"
        for row in rows
    )
    head = "<tr><th>Код</th><th>Месяц</th><th>Дней</th><th>Средний</th><th>Минимум</th><th>Максимум</th></tr>"
    return f"<h2>По месяцам</h2><table>{head}{body}</table>"
