from __future__ import annotations

from datetime import date

import pytest
from conftest import FRIDAY

from cbr.flow import Context, Dag, FlowError, Task
from cbr.store import BLOCKED, DONE, FAILED, SKIPPED, Store


def note(name: str, log: list[str]) -> Task:
    def run(context: Context) -> str:
        log.append(name)
        return f"{name} за {context.day:%d.%m}"

    return Task(name, run, retries=0)


def test_order_follows_dependencies() -> None:
    log: list[str] = []
    dag = Dag(
        "тест",
        [
            Task("третья", note("третья", log).run, depends_on=("вторая",), retries=0),
            Task("первая", note("первая", log).run, retries=0),
            Task("вторая", note("вторая", log).run, depends_on=("первая",), retries=0),
        ],
    )
    assert [task.name for task in dag.order] == ["первая", "вторая", "третья"]


@pytest.mark.parametrize(
    ("tasks", "message"),
    [
        ([], "хотя бы одна задача"),
        ([Task("а", lambda context: "", depends_on=("нет",))], "несуществующих"),
        ([Task("а", lambda context: ""), Task("а", lambda context: "")], "повторяются"),
        (
            [
                Task("а", lambda context: "", depends_on=("б",)),
                Task("б", lambda context: "", depends_on=("а",)),
            ],
            "кольцо",
        ),
    ],
)
def test_broken_graph_is_refused(tasks: list[Task], message: str) -> None:
    with pytest.raises(FlowError, match=message):
        Dag("тест", tasks)


def test_every_task_runs_once(store: Store) -> None:
    log: list[str] = []
    dag = Dag("тест", [note("первая", log), Task("вторая", note("вторая", log).run, depends_on=("первая",))])
    report = dag.run_for(FRIDAY, store)
    assert log == ["первая", "вторая"]
    assert [result.status for result in report.results] == [DONE, DONE]
    assert report.ok


def test_finished_task_is_not_repeated(store: Store) -> None:
    log: list[str] = []
    dag = Dag("тест", [note("первая", log)])
    dag.run_for(FRIDAY, store)
    again = dag.run_for(FRIDAY, store)
    assert log == ["первая"], "второй запуск того же дня не должен выполнять задачу заново"
    assert again.results[0].status == SKIPPED


def test_force_runs_it_again(store: Store) -> None:
    log: list[str] = []
    dag = Dag("тест", [note("первая", log)])
    dag.run_for(FRIDAY, store)
    dag.run_for(FRIDAY, store, force=True)
    assert log == ["первая", "первая"]


def test_failure_is_retried_with_growing_pause(store: Store) -> None:
    attempts = {"count": 0}
    pauses: list[float] = []

    def flaky(context: Context) -> str:
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise RuntimeError("источник молчит")
        return "получилось с третьей попытки"

    dag = Dag("тест", [Task("шаткая", flaky, retries=2, retry_delay=2.0)])
    report = dag.run_for(FRIDAY, store, sleep=pauses.append)

    assert report.results[0].status == DONE
    assert report.results[0].attempts == 3
    assert pauses == [2.0, 4.0]


def test_hopeless_task_fails_and_blocks_the_rest(store: Store) -> None:
    def broken(context: Context) -> str:
        raise RuntimeError("совсем плохо")

    log: list[str] = []
    dag = Dag(
        "тест",
        [
            Task("шаткая", broken, retries=1, retry_delay=0.0),
            Task("следом", note("следом", log).run, depends_on=("шаткая",)),
        ],
    )
    report = dag.run_for(FRIDAY, store, sleep=lambda seconds: None)

    assert [result.status for result in report.results] == [FAILED, BLOCKED]
    assert log == [], "зависимая задача не должна запускаться после падения"
    assert not report.ok
    assert "совсем плохо" in report.failed[0].message


def test_backfill_walks_day_by_day(store: Store) -> None:
    days: list[date] = []
    dag = Dag("тест", [Task("шаг", lambda context: str(days.append(context.day) or "ок"), retries=0)])
    reports = dag.backfill(date(2026, 2, 2), date(2026, 2, 5), store)
    assert len(reports) == 4
    assert days == [date(2026, 2, day) for day in (2, 3, 4, 5)]


def test_backfill_refuses_reversed_period(store: Store) -> None:
    dag = Dag("тест", [Task("шаг", lambda context: "ок")])
    with pytest.raises(FlowError, match="позже"):
        dag.backfill(date(2026, 3, 1), date(2026, 2, 1), store)
