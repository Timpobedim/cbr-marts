"""Хранилище: сырой журнал, факт курсов, витрины и журнал запусков конвейера.

Колоночная база DuckDB вместо ClickHouse: запросы здесь такие же аналитические
(«средний курс по месяцам», «изменение за неделю»), но поднимать сервер ради сорока валют не нужно.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb

from cbr.source import DailyRates

DDL = (
    """
    CREATE TABLE IF NOT EXISTS raw_answer (
        requested DATE,
        actual DATE,
        fetched_at TIMESTAMP,
        size_bytes INTEGER,
        content_hash VARCHAR,
        path VARCHAR,
        PRIMARY KEY (requested)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_currency (
        code VARCHAR PRIMARY KEY,
        name VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_rate (
        rate_date DATE,
        code VARCHAR,
        nominal INTEGER,
        value DECIMAL(18,4),
        per_unit DECIMAL(18,6),
        PRIMARY KEY (rate_date, code)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS run_journal (
        dag VARCHAR,
        task VARCHAR,
        run_date DATE,
        attempt INTEGER,
        status VARCHAR,
        message VARCHAR,
        seconds DOUBLE,
        finished_at TIMESTAMP
    )
    """,
)

DONE = "готово"
SKIPPED = "пропущено"
FAILED = "ошибка"
BLOCKED = "не запускалась"


class StoreError(RuntimeError):
    """Хранилище недоступно или запрос не выполнился."""


@dataclass(frozen=True, slots=True)
class RateRow:
    rate_date: date
    code: str
    per_unit: Decimal


@dataclass(frozen=True, slots=True)
class JournalRow:
    dag: str
    task: str
    run_date: date
    attempt: int
    status: str
    message: str
    seconds: float
    finished_at: datetime


class Store:
    """Один файл DuckDB: сырой журнал, факт, витрины и история запусков."""

    def __init__(self, path: Path | str = ":memory:") -> None:
        if str(path) != ":memory:":
            # Папку создаём сами: иначе DuckDB отвечает «не удаётся найти указанный путь», и человеку
            # остаётся гадать, чего именно не нашлось — файла, диска или прав.
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        try:
            self.connection = duckdb.connect(str(path))
        except duckdb.Error as exc:
            raise StoreError(f"Не удалось открыть хранилище {path}: {exc}") from exc
        for statement in DDL:
            self.execute(statement)

    # ── сырой слой и факт ───────────────────────────────────────────────────────────────────────

    def record_answer(self, daily: DailyRates, *, size: int, content_hash: str, path: str) -> None:
        self.execute(
            """
            INSERT INTO raw_answer VALUES (?,?,?,?,?,?)
            ON CONFLICT (requested) DO UPDATE SET
                actual = excluded.actual,
                fetched_at = excluded.fetched_at,
                size_bytes = excluded.size_bytes,
                content_hash = excluded.content_hash,
                path = excluded.path
            """,
            [
                daily.requested,
                daily.actual,
                datetime.now(UTC).replace(tzinfo=None),
                size,
                content_hash,
                path,
            ],
        )

    def upsert_rates(self, daily: DailyRates) -> int:
        """Курсы кладутся по дате из ответа: за выходной день строки просто перезапишутся теми же."""
        self.executemany(
            "INSERT INTO dim_currency VALUES (?, ?) ON CONFLICT (code) DO UPDATE SET name = excluded.name",
            [[rate.code, rate.name] for rate in daily.rates],
        )
        self.executemany(
            """
            INSERT INTO fact_rate VALUES (?,?,?,?,?)
            ON CONFLICT (rate_date, code) DO UPDATE SET
                nominal = excluded.nominal,
                value = excluded.value,
                per_unit = excluded.per_unit
            """,
            [[daily.actual, rate.code, rate.nominal, rate.value, rate.per_unit] for rate in daily.rates],
        )
        return len(daily.rates)

    def rates(self, code: str, *, since: date | None = None, until: date | None = None) -> list[RateRow]:
        where = ["code = ?"]
        parameters: list[object] = [code]
        if since:
            where.append("rate_date >= ?")
            parameters.append(since)
        if until:
            where.append("rate_date <= ?")
            parameters.append(until)
        rows = self.query(
            f"SELECT rate_date, code, per_unit FROM fact_rate WHERE {' AND '.join(where)} ORDER BY rate_date",  # noqa: S608
            parameters,
        )
        return [RateRow(rate_date=row[0], code=str(row[1]), per_unit=Decimal(str(row[2]))) for row in rows]

    def period(self) -> tuple[date | None, date | None]:
        first, last = self.one("SELECT min(rate_date), max(rate_date) FROM fact_rate")
        return first, last

    # ── журнал запусков ─────────────────────────────────────────────────────────────────────────

    def task_status(self, dag: str, task: str, run_date: date) -> str | None:
        rows = self.query(
            """
            SELECT status FROM run_journal
            WHERE dag = ? AND task = ? AND run_date = ?
            ORDER BY attempt DESC, finished_at DESC LIMIT 1
            """,
            [dag, task, run_date],
        )
        return str(rows[0][0]) if rows else None

    def record_attempt(
        self,
        *,
        dag: str,
        task: str,
        run_date: date,
        attempt: int,
        status: str,
        message: str,
        seconds: float,
    ) -> None:
        self.execute(
            "INSERT INTO run_journal VALUES (?,?,?,?,?,?,?,?)",
            [
                dag,
                task,
                run_date,
                attempt,
                status,
                message[:500],
                round(seconds, 3),
                datetime.now(UTC).replace(tzinfo=None),
            ],
        )

    def journal(self, *, limit: int = 30, run_date: date | None = None) -> list[JournalRow]:
        where = "WHERE run_date = ?" if run_date else ""
        parameters: list[object] = [run_date] if run_date else []
        parameters.append(limit)
        rows = self.query(
            f"SELECT * FROM run_journal {where} ORDER BY finished_at DESC, task LIMIT ?",  # noqa: S608
            parameters,
        )
        rows.reverse()
        return [
            JournalRow(
                dag=str(row[0]),
                task=str(row[1]),
                run_date=row[2],
                attempt=int(row[3]),
                status=str(row[4]),
                message=str(row[5]),
                seconds=float(row[6]),
                finished_at=row[7],
            )
            for row in rows
        ]

    def done_days(self, dag: str, task: str) -> set[date]:
        rows = self.query(
            "SELECT DISTINCT run_date FROM run_journal WHERE dag = ? AND task = ? AND status = ?",
            [dag, task, DONE],
        )
        return {row[0] for row in rows}

    # ── низкий уровень ──────────────────────────────────────────────────────────────────────────

    def execute(self, sql: str, parameters: Sequence[object] | None = None) -> None:
        try:
            self.connection.execute(sql, list(parameters) if parameters else None)
        except duckdb.Error as exc:
            raise StoreError(f"Запрос не выполнился: {exc}") from exc

    def executemany(self, sql: str, rows: Sequence[Sequence[object]]) -> None:
        if not rows:
            return
        try:
            self.connection.executemany(sql, [list(row) for row in rows])
        except duckdb.Error as exc:
            raise StoreError(f"Запрос не выполнился: {exc}") from exc

    def query(self, sql: str, parameters: Sequence[object] | None = None) -> list[tuple[Any, ...]]:
        try:
            return list(self.connection.execute(sql, list(parameters) if parameters else None).fetchall())
        except duckdb.Error as exc:
            raise StoreError(f"Запрос не выполнился: {exc}") from exc

    def one(self, sql: str, parameters: Sequence[object] | None = None) -> tuple[Any, ...]:
        rows = self.query(sql, parameters)
        if not rows:
            raise StoreError("Запрос не вернул ни одной строки")
        return rows[0]

    def count(self, table: str, where: str = "") -> int:
        condition = f" WHERE {where}" if where else ""
        return int(self.one(f"SELECT count(*) FROM {table}{condition}")[0])  # noqa: S608 — имена таблиц свои

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> Store:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
