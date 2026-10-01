"""Демонстрация: догрузка за месяц, повторный запуск и страница со сводкой.

По умолчанию демонстрация ходит в сеть — источник открытый, без ключей и ограничений. С ключом
`--offline` вместо сети используется сырой слой, заполненный правдоподобными ответами: так демо
работает в самолёте и в проверках, а конвейер при этом запускается настоящий, целиком.
"""

from __future__ import annotations

import random
import tempfile
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from cbr import report
from cbr.flow import RunReport
from cbr.pipeline import make_dag
from cbr.source import Client
from cbr.store import Store

DEMO_CURRENCIES = (
    ("R01235", "USD", 1, "Доллар США", Decimal("84.25")),
    ("R01239", "EUR", 1, "Евро", Decimal("97.87")),
    ("R01375", "CNY", 1, "Китайский юань", Decimal("12.55")),
    ("R01035", "GBP", 1, "Фунт стерлингов", Decimal("114.00")),
    ("R01335", "KZT", 100, "Казахстанских тенге", Decimal("18.69")),
    ("R01820", "JPY", 100, "Японских иен", Decimal("57.30")),
)


@dataclass(frozen=True, slots=True)
class DemoResult:
    root: Path
    reports: list[RunReport]
    repeat: RunReport
    text: str
    page: Path


def run(*, root: Path | None = None, days: int = 30, offline: bool = False, today: date | None = None) -> DemoResult:
    base = Path(root) if root else Path(tempfile.mkdtemp(prefix="cbr-demo-"))
    base.mkdir(parents=True, exist_ok=True)
    raw = base / "сырые-ответы"
    warehouse = base / "курсы.duckdb"
    warehouse.unlink(missing_ok=True)

    last = today or datetime.now(UTC).date()
    since = last - timedelta(days=days - 1)
    if offline:
        fill_raw(raw, since, last)

    client = Client(cache=raw, pause=0.2)
    dag = make_dag(client)
    with Store(warehouse) as store:
        reports = dag.backfill(since, last, store)
        repeat = dag.run_for(last, store)  # второй запуск того же дня: всё должно быть пропущено
        page = base / "сводка.html"
        page.write_text(report.dashboard(store, generated_on=last), "utf-8")
        text = "\n".join(
            [
                f"Догрузка с {since:%d.%m.%Y} по {last:%d.%m.%Y}: дней {len(reports)}, "
                f"из них с ошибками {sum(1 for item in reports if not item.ok)}.",
                f"Выходных, где ЦБ отдал курсы предыдущего рабочего дня: {_moved(reports)}.",
                "",
                f"Повторный запуск за {last:%d.%m.%Y}: " + ", ".join(f"{r.task} — {r.status}" for r in repeat.results),
                "",
                report.summary(store, journal=store.journal(limit=8)),
                "",
                f"Страница сводки: {page}",
                f"Файлы демонстрации: {base}",
            ]
        )
    return DemoResult(root=base, reports=reports, repeat=repeat, text=text, page=page)


def _moved(reports: list[RunReport]) -> int:
    return sum(1 for item in reports for result in item.results if "выходной" in result.message)


def fill_raw(raw: Path, since: date, until: date, *, seed: int = 5) -> int:
    """Заполняет сырой слой правдоподобными ответами, включая поведение ЦБ в выходные."""
    raw.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    values = {code: value for _, code, _, _, value in DEMO_CURRENCIES}
    written = 0
    day = since
    while day <= until:
        for code in values:
            values[code] = (values[code] * Decimal(str(1 + rng.uniform(-0.012, 0.012)))).quantize(Decimal("0.0001"))
        actual = _business_day(day)
        payload = _xml(actual, values)
        (raw / f"{day:%Y-%m-%d}.xml").write_bytes(payload)
        written += 1
        day += timedelta(days=1)
    return written


def _business_day(day: date) -> date:
    """ЦБ не публикует курсы в выходные: ответ приходит с датой предыдущего рабочего дня."""
    shift = {5: 1, 6: 2}.get(day.weekday(), 0)
    return day - timedelta(days=shift)


def _xml(actual: date, values: dict[str, Decimal]) -> bytes:
    parts = [
        '<?xml version="1.0" encoding="windows-1251"?>'
        f'<ValCurs Date="{actual:%d.%m.%Y}" name="Foreign Currency Market">'
    ]
    for code_id, code, nominal, name, _ in DEMO_CURRENCIES:
        value = f"{values[code] * nominal:.4f}".replace(".", ",")
        parts.append(
            f'<Valute ID="{code_id}"><NumCode>000</NumCode><CharCode>{code}</CharCode>'
            f"<Nominal>{nominal}</Nominal><Name>{name}</Name><Value>{value}</Value>"
            f"<VunitRate>{value}</VunitRate></Valute>"
        )
    parts.append("</ValCurs>")
    return "".join(parts).encode("cp1251", errors="replace")
