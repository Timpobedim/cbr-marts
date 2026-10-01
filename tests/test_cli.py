from __future__ import annotations

from pathlib import Path

import pytest
from conftest import FIRST, FRIDAY, LAST

from cbr.cli import main


@pytest.fixture
def stand(tmp_path: Path, raw: Path) -> tuple[Path, Path]:
    warehouse = tmp_path / "курсы.duckdb"
    assert (
        main(
            [
                "backfill",
                "--warehouse",
                str(warehouse),
                "--raw",
                str(raw),
                "--since",
                f"{FIRST:%d.%m.%Y}",
                "--until",
                f"{LAST:%d.%m.%Y}",
            ]
        )
        == 0
    )
    return warehouse, raw


def test_single_day_runs(tmp_path: Path, raw: Path, capsys: pytest.CaptureFixture[str]) -> None:
    warehouse = tmp_path / "курсы.duckdb"
    code = main(["run", "--warehouse", str(warehouse), "--raw", str(raw), "--date", f"{FRIDAY:%d.%m.%Y}"])
    printed = capsys.readouterr().out
    assert code == 0
    assert "извлечь: готово" in printed
    assert "проверить: готово" in printed


def test_repeat_run_is_skipped(stand: tuple[Path, Path], capsys: pytest.CaptureFixture[str]) -> None:
    warehouse, raw = stand
    capsys.readouterr()
    assert main(["run", "--warehouse", str(warehouse), "--raw", str(raw), "--date", f"{LAST:%d.%m.%Y}"]) == 0
    assert "пропущено" in capsys.readouterr().out


def test_report_and_dashboard(stand: tuple[Path, Path], tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    warehouse, raw = stand
    assert main(["report", "--warehouse", str(warehouse), "--raw", str(raw)]) == 0
    printed = capsys.readouterr().out
    assert "Изменения:" in printed
    assert "по месяцам" in printed

    page = tmp_path / "сводка.html"
    assert main(["dashboard", "--warehouse", str(warehouse), "--raw", str(raw), "--out", str(page)]) == 0
    assert "Курсы Банка России" in page.read_text("utf-8")


def test_status_shows_the_journal(stand: tuple[Path, Path], capsys: pytest.CaptureFixture[str]) -> None:
    warehouse, raw = stand
    assert main(["status", "--warehouse", str(warehouse), "--raw", str(raw), "--limit", "5"]) == 0
    printed = capsys.readouterr().out
    assert "Задача" in printed
    assert "готово" in printed


def test_empty_warehouse_is_explained(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["report", "--warehouse", str(tmp_path / "пусто.duckdb"), "--raw", str(tmp_path / "нет")]) == 0
    assert "пока нет курсов" in capsys.readouterr().out


def test_bad_date_is_refused(tmp_path: Path) -> None:
    with pytest.raises(SystemExit):
        main(["run", "--warehouse", str(tmp_path / "пусто.duckdb"), "--date", "вчера"])


def test_demo_without_network(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    code = main(["demo", "--root", str(tmp_path / "демо"), "--days", "12", "--offline"])
    printed = capsys.readouterr().out
    assert code == 0
    assert "Повторный запуск" in printed
    assert "пропущено" in printed
    assert (tmp_path / "демо" / "сводка.html").exists()
