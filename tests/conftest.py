from __future__ import annotations

from collections.abc import Iterator, Mapping
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from cbr.demo import fill_raw
from cbr.source import Client
from cbr.store import Store

RATES: dict[str, tuple[int, Decimal, str]] = {
    "USD": (1, Decimal("84.2569"), "Доллар США"),
    "EUR": (1, Decimal("97.8728"), "Евро"),
    "CNY": (1, Decimal("12.5519"), "Китайский юань"),
    "KZT": (100, Decimal("18.6909"), "Казахстанских тенге"),
}

FIRST = date(2026, 2, 2)
LAST = date(2026, 3, 6)
FRIDAY = date(2026, 2, 6)
SATURDAY = date(2026, 2, 7)


def payload(actual: date, rates: Mapping[str, tuple[int, Decimal, str]] | None = None) -> bytes:
    """Ответ ЦБ в том же виде, в каком его отдаёт сервер: windows-1251 и запятая в числах."""
    items = rates if rates is not None else RATES
    parts = [
        '<?xml version="1.0" encoding="windows-1251"?>'
        f'<ValCurs Date="{actual:%d.%m.%Y}" name="Foreign Currency Market">'
    ]
    for code, (nominal, value, name) in items.items():
        text = f"{value:.4f}".replace(".", ",")
        parts.append(
            f'<Valute ID="R0{code}"><NumCode>000</NumCode><CharCode>{code}</CharCode>'
            f"<Nominal>{nominal}</Nominal><Name>{name}</Name><Value>{text}</Value>"
            f"<VunitRate>{text}</VunitRate></Valute>"
        )
    parts.append("</ValCurs>")
    return "".join(parts).encode("cp1251", errors="replace")


@pytest.fixture
def store() -> Iterator[Store]:
    warehouse = Store(":memory:")
    yield warehouse
    warehouse.close()


@pytest.fixture
def raw(tmp_path: Path) -> Path:
    """Сырой слой, заполненный ответами за месяц: сеть в проверках не нужна."""
    folder = tmp_path / "сырые-ответы"
    fill_raw(folder, FIRST, LAST, seed=3)
    return folder


@pytest.fixture
def client(raw: Path) -> Client:
    return Client(cache=raw, pause=0.0)
