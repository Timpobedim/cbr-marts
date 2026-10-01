"""Командная строка: run, backfill, report, dashboard, status, demo."""

from __future__ import annotations

import argparse
import io
import sys
from collections.abc import Sequence
from datetime import UTC, date, datetime
from pathlib import Path

from cbr import demo as demo_module
from cbr import report
from cbr.flow import FlowError, RunReport
from cbr.pipeline import make_dag
from cbr.source import Client, SourceError
from cbr.store import Store, StoreError

WAREHOUSE = "курсы.duckdb"
RAW = "сырые-ответы"


def main(argv: Sequence[str] | None = None) -> int:
    _utf8_console()
    parser = _parser()
    args = parser.parse_args(argv)
    try:
        return int(args.handler(args))
    except (StoreError, SourceError, FlowError, ValueError) as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        return 2


def _utf8_console() -> None:
    """В стандартной консоли Windows нет знака рубля: без этого отчёт падает с UnicodeEncodeError."""
    for stream in (sys.stdout, sys.stderr):
        if isinstance(stream, io.TextIOWrapper) and stream.encoding.lower() not in {"utf-8", "utf8"}:
            stream.reconfigure(encoding="utf-8", errors="replace")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="cbr", description="Ежедневный ETL курсов Банка России")
    commands = parser.add_subparsers(dest="command", required=True)

    run = commands.add_parser("run", help="прогнать конвейер за один день")
    _common(run)
    run.add_argument("--date", type=_day, help="день, ДД.ММ.ГГГГ (по умолчанию сегодня)")
    run.add_argument("--force", action="store_true", help="выполнить задачи заново, даже если они уже сделаны")
    run.set_defaults(handler=_run)

    backfill = commands.add_parser("backfill", help="догрузить период")
    _common(backfill)
    backfill.add_argument("--since", type=_day, required=True)
    backfill.add_argument("--until", type=_day, required=True)
    backfill.add_argument("--force", action="store_true")
    backfill.set_defaults(handler=_backfill)

    show = commands.add_parser("report", help="отчёт по хранилищу")
    _common(show)
    show.add_argument("--code", default="USD", help="валюта для помесячной таблицы")
    show.set_defaults(handler=_report)

    page = commands.add_parser("dashboard", help="собрать HTML-страницу со сводкой")
    _common(page)
    page.add_argument("--out", type=Path, required=True)
    page.set_defaults(handler=_dashboard)

    status = commands.add_parser("status", help="журнал запусков")
    _common(status)
    status.add_argument("--date", type=_day)
    status.add_argument("--limit", type=int, default=20)
    status.set_defaults(handler=_status)

    show_demo = commands.add_parser("demo", help="догрузка за месяц, повторный запуск и страница")
    show_demo.add_argument("--root", type=Path)
    show_demo.add_argument("--days", type=int, default=30)
    show_demo.add_argument("--offline", action="store_true", help="без сети: ответы берутся из сырого слоя")
    show_demo.set_defaults(handler=_demo)
    return parser


def _common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--warehouse", type=Path, default=Path(WAREHOUSE))
    parser.add_argument("--raw", type=Path, default=Path(RAW), help="папка сырых ответов")


def _run(args: argparse.Namespace) -> int:
    day = args.date or datetime.now(UTC).date()
    with Store(args.warehouse) as store:
        dag = make_dag(Client(cache=args.raw))
        report_run = dag.run_for(day, store, force=args.force)
    _print_run(report_run)
    return 0 if report_run.ok else 1


def _backfill(args: argparse.Namespace) -> int:
    with Store(args.warehouse) as store:
        dag = make_dag(Client(cache=args.raw))
        reports = dag.backfill(args.since, args.until, store, force=args.force)
    bad = [item for item in reports if not item.ok]
    print(f"Дней обработано: {len(reports)}, с ошибками: {len(bad)}.")
    for item in bad:
        _print_run(item)
    return 0 if not bad else 1


def _report(args: argparse.Namespace) -> int:
    with Store(args.warehouse) as store:
        first, _ = store.period()
        if first is None:
            print("В хранилище пока нет курсов: запустите «cbr run» или «cbr backfill».")
            return 0
        print(report.summary(store, journal=store.journal(limit=6)))
        print()
        print(f"{args.code} по месяцам:")
        print(report.months_table(store, args.code))
    return 0


def _dashboard(args: argparse.Namespace) -> int:
    with Store(args.warehouse) as store:
        Path(args.out).write_text(report.dashboard(store), "utf-8")
    print(f"Страница сохранена: {args.out}")
    return 0


def _status(args: argparse.Namespace) -> int:
    with Store(args.warehouse) as store:
        rows = store.journal(limit=args.limit, run_date=args.date)
    if not rows:
        print("Журнал пуст.")
        return 0
    print(report.journal_table(rows))
    return 0


def _demo(args: argparse.Namespace) -> int:
    result = demo_module.run(root=args.root, days=args.days, offline=args.offline)
    print(result.text)
    return 0 if all(item.ok for item in result.reports) else 1


def _print_run(run_report: RunReport) -> None:
    print(run_report.line)
    for result in run_report.results:
        print(f"  {result.task}: {result.status} — {result.message}")


def _day(text: str) -> date:
    try:
        return datetime.strptime(text, "%d.%m.%Y").date()  # noqa: DTZ007 — календарная дата, не момент
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"дата в формате ДД.ММ.ГГГГ, получено «{text}»") from exc


if __name__ == "__main__":
    raise SystemExit(main())
