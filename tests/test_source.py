from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
from conftest import FRIDAY, SATURDAY, payload

from cbr.source import USER_AGENT, Client, SourceError, parse


def test_rates_are_read_with_nominal() -> None:
    daily = parse(payload(FRIDAY), FRIDAY)
    assert daily.actual == FRIDAY
    assert not daily.moved

    dollar = daily.by_code("USD")
    assert dollar is not None
    assert dollar.value == Decimal("84.2569")
    assert dollar.per_unit == Decimal("84.256900")

    tenge = daily.by_code("KZT")
    assert tenge is not None
    assert tenge.nominal == 100
    assert tenge.per_unit == Decimal("0.186909"), "курс за одну единицу, а не за сотню"


def test_cyrillic_names_survive_the_encoding() -> None:
    daily = parse(payload(FRIDAY), FRIDAY)
    dollar = daily.by_code("USD")
    assert dollar is not None
    assert dollar.name == "Доллар США"


def test_weekend_answer_keeps_the_business_day() -> None:
    """Главная особенность источника: в субботу приходят пятничные курсы с пятничной датой."""
    daily = parse(payload(FRIDAY), SATURDAY)
    assert daily.requested == SATURDAY
    assert daily.actual == FRIDAY
    assert daily.moved


def test_error_answer_is_recognised() -> None:
    broken = '<?xml version="1.0" encoding="windows-1251"?><ValCurs>\nError in parameters\n</ValCurs>'.encode("cp1251")
    with pytest.raises(SourceError, match="ответил отказом"):
        parse(broken, FRIDAY)


def test_unreadable_xml_is_explained() -> None:
    with pytest.raises(SourceError, match="не разобрать как XML"):
        parse(b"<ValCurs", FRIDAY)


def test_empty_answer_is_refused() -> None:
    empty = f'<ValCurs Date="{FRIDAY:%d.%m.%Y}"></ValCurs>'.encode("cp1251")
    with pytest.raises(SourceError, match="нет ни одной валюты"):
        parse(empty, FRIDAY)


def test_nonsense_numbers_are_refused() -> None:
    rates = {"USD": (1, Decimal("0"), "Доллар США")}
    with pytest.raises(SourceError, match="положительными"):
        parse(payload(FRIDAY, rates), FRIDAY)


def test_saved_answer_is_used_instead_of_network(tmp_path: Path) -> None:
    cache = tmp_path / "сырые"
    cache.mkdir()
    (cache / f"{FRIDAY:%Y-%m-%d}.xml").write_bytes(payload(FRIDAY))

    def refuse(request: httpx.Request) -> httpx.Response:
        raise AssertionError("сохранённый ответ не должен ходить в сеть")

    client = Client(cache=cache, client=httpx.Client(transport=httpx.MockTransport(refuse)))
    assert client.daily(FRIDAY).actual == FRIDAY


def test_downloaded_answer_is_saved(tmp_path: Path) -> None:
    cache = tmp_path / "сырые"
    client = Client(
        cache=cache,
        client=httpx.Client(
            transport=httpx.MockTransport(lambda request: httpx.Response(200, content=payload(FRIDAY)))
        ),
    )
    client.daily(FRIDAY)
    assert (cache / f"{FRIDAY:%Y-%m-%d}.xml").exists(), "сырой ответ должен остаться на диске"


def test_temporary_failure_is_retried(tmp_path: Path) -> None:
    attempts = {"count": 0}

    def flaky(request: httpx.Request) -> httpx.Response:
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise httpx.ConnectError("сеть моргнула")
        return httpx.Response(200, content=payload(FRIDAY))

    client = Client(
        cache=tmp_path / "сырые",
        client=httpx.Client(transport=httpx.MockTransport(flaky)),
        pause=0.0,
        sleep=lambda seconds: None,
    )
    assert client.daily(FRIDAY).actual == FRIDAY
    assert attempts["count"] == 3


def test_permanent_failure_names_the_day(tmp_path: Path) -> None:
    def broken(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("сети нет")

    client = Client(
        cache=tmp_path / "сырые",
        client=httpx.Client(transport=httpx.MockTransport(broken)),
        pause=0.0,
        sleep=lambda seconds: None,
    )
    with pytest.raises(SourceError, match=r"06\.02\.2026"):
        client.daily(date(2026, 2, 6))


def test_server_error_is_not_taken_for_data(tmp_path: Path) -> None:
    client = Client(
        cache=tmp_path / "сырые",
        client=httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(503))),
        pause=0.0,
        sleep=lambda seconds: None,
    )
    with pytest.raises(SourceError):
        client.daily(FRIDAY)


def test_user_agent_is_latin_only() -> None:
    """Заголовки HTTP кодируются в latin-1: кириллица в User-Agent роняет запрос ещё до сети.

    Проверки с эмулятором этого не ловили — они подставляют своего клиента, без наших заголовков,
    и ошибка нашлась только на живом прогоне.
    """
    assert USER_AGENT.isascii()
