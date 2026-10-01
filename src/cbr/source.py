"""Источник: ежедневные курсы Банка России.

Публичный XML без ключей и регистрации: `https://www.cbr.ru/scripts/XML_daily.asp?date_req=ДД/ММ/ГГГГ`.
Две особенности этого источника определяют устройство всего конвейера.

Первая: в выходной день ЦБ отдаёт курсы предыдущего рабочего дня, и в ответе стоит именно его дата.
Проверено 12.09.2026: запросы за 13 и 14 сентября вернули `ValCurs Date="12.09.2026"`. Если складывать
ответы по запрошенной дате, одни и те же курсы лягут в хранилище трижды, а недельная динамика поедет.
Поэтому факт хранится по дате из ответа, а по запрошенной — только запись в журнале.

Вторая: на неверную дату сервер отвечает не ошибкой, а строкой «Error in parameters» с кодом 200.
Такой ответ здесь распознаётся и превращается в понятную ошибку.
"""

from __future__ import annotations

import hashlib
import os
import time

# Разбор стандартным ElementTree: это XML известного сервера, а не файл, присланный пользователем.
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path

import httpx

URL = "https://www.cbr.ru/scripts/XML_daily.asp"
TIMEOUT = httpx.Timeout(connect=5.0, read=20.0, write=10.0, pool=5.0)
# Заголовок только латиницей: HTTP-заголовки передаются в latin-1, и кириллица в User-Agent
# роняет запрос с UnicodeEncodeError ещё до выхода в сеть.
USER_AGENT = "cbr-marts/1.0 (educational project; daily CBR rates)"
RETRIES = 2
UNIT = Decimal("0.000001")


class SourceError(RuntimeError):
    """Источник не ответил или ответил не тем; текст — для человека."""


@dataclass(frozen=True, slots=True)
class Rate:
    """Курс одной валюты. У ЦБ курс указан за номинал: тенге — за 100, иена — за 100."""

    code: str
    name: str
    nominal: int
    value: Decimal

    @property
    def per_unit(self) -> Decimal:
        """Курс за одну единицу — то, что нужно витринам, чтобы валюты можно было сравнивать."""
        return (self.value / self.nominal).quantize(UNIT)


@dataclass(frozen=True, slots=True)
class DailyRates:
    """Ответ источника за один день."""

    requested: date
    actual: date
    rates: tuple[Rate, ...]

    @property
    def moved(self) -> bool:
        """Ответ съехал на предыдущий рабочий день — это выходной или праздник."""
        return self.actual != self.requested

    def by_code(self, code: str) -> Rate | None:
        return next((rate for rate in self.rates if rate.code == code), None)


def parse(payload: bytes, requested: date) -> DailyRates:
    """Разбирает ответ ЦБ. Кодировка в объявлении XML — windows-1251, поэтому разбираем байты."""
    try:
        root = ET.fromstring(payload)  # noqa: S314 — ответ известного сервера, внешние сущности ElementTree не подставляет
    except ET.ParseError as exc:
        raise SourceError(f"Ответ ЦБ не разобрать как XML: {exc}") from exc

    stamp = root.get("Date")
    if not stamp:
        message = (root.text or "").strip() or "в ответе нет даты"
        raise SourceError(f"ЦБ ответил отказом: {message}")
    try:
        actual = _day(stamp)
    except ValueError as exc:
        raise SourceError(f"Непонятная дата в ответе: «{stamp}»") from exc

    rates: list[Rate] = []
    for node in root.findall("Valute"):
        code = _text(node, "CharCode")
        raw_value = _text(node, "Value")
        raw_nominal = _text(node, "Nominal")
        if not code or not raw_value or not raw_nominal:
            continue
        try:
            value = Decimal(raw_value.replace(",", "."))
            nominal = int(raw_nominal)
        except (InvalidOperation, ValueError) as exc:
            raise SourceError(f"Валюта {code}: не разобрать курс «{raw_value}» или номинал «{raw_nominal}»") from exc
        if nominal <= 0 or value <= 0:
            raise SourceError(f"Валюта {code}: номинал и курс должны быть положительными")
        rates.append(Rate(code=code, name=_text(node, "Name") or code, nominal=nominal, value=value))

    if not rates:
        raise SourceError(f"В ответе за {actual:%d.%m.%Y} нет ни одной валюты")
    return DailyRates(requested=requested, actual=actual, rates=tuple(rates))


class Client:
    """Клиент ЦБ с сырым слоем на диске: скачанный ответ сохраняется как есть.

    Сырой слой — не украшение. Если завтра выяснится, что курс разобран неправильно, пересобрать
    хранилище можно из сохранённых ответов, не выпрашивая у источника историю заново.
    """

    def __init__(
        self,
        *,
        cache: Path | None = None,
        client: httpx.Client | None = None,
        retries: int = RETRIES,
        pause: float = 1.0,
        sleep: object = None,
    ) -> None:
        self.cache = cache
        self._client = client
        self.retries = retries
        self.pause = pause
        self._sleep = sleep if callable(sleep) else time.sleep

    def daily(self, day: date) -> DailyRates:
        payload = self.payload(day)
        return parse(payload, day)

    def payload(self, day: date) -> bytes:
        """Сырой ответ: из кеша, если он есть, иначе из сети — и тогда сразу в кеш."""
        path = self.raw_path(day)
        if path is not None and path.exists():
            try:
                return path.read_bytes()
            except OSError as exc:
                raise SourceError(f"Не прочитать сохранённый ответ {path.name}: {exc}") from exc
        payload = self._download(day)
        self._save(day, payload)
        return payload

    def raw_path(self, day: date) -> Path | None:
        return None if self.cache is None else self.cache / f"{day:%Y-%m-%d}.xml"

    def _download(self, day: date) -> bytes:
        client = self._client or httpx.Client(timeout=TIMEOUT, headers={"User-Agent": USER_AGENT})
        last: Exception | None = None
        try:
            for attempt in range(self.retries + 1):
                try:
                    response = client.get(URL, params={"date_req": f"{day:%d/%m/%Y}"})
                    response.raise_for_status()
                    return bytes(response.content)
                except httpx.HTTPError as exc:
                    last = exc
                    if attempt < self.retries:
                        self._sleep(self.pause * 2.0**attempt)
        finally:
            if self._client is None:
                client.close()
        raise SourceError(f"ЦБ не ответил за {day:%d.%m.%Y}: {last}")

    def _save(self, day: date, payload: bytes) -> None:
        path = self.raw_path(day)
        if path is None:
            return
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(".tmp")
            tmp.write_bytes(payload)
            os.replace(tmp, path)
        except OSError as exc:
            raise SourceError(f"Не сохранить ответ за {day:%d.%m.%Y}: {exc}") from exc


def digest(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _text(node: ET.Element, tag: str) -> str:
    child = node.find(tag)
    return (child.text or "").strip() if child is not None else ""


def _day(stamp: str) -> date:
    parts = stamp.split(".")
    if len(parts) != 3:
        raise ValueError(stamp)
    return date(int(parts[2]), int(parts[1]), int(parts[0]))
