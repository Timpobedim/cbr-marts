"""Мини-оркестратор: задачи с зависимостями, повторы, журнал и догрузка за прошлое.

Это то, ради чего обычно поднимают Airflow: расписание, зависимости между
шагами, повторы при сбое и возможность перезапустить вчерашний день. Airflow делает это хорошо,
но вместе с ним приходят веб-сервер, планировщик, база метаданных и контейнеры — ради трёх задач,
которые выполняются за секунду.

Здесь то же самое в полутора сотнях строк: порядок задач считается из зависимостей, каждая попытка
пишется в журнал, успешная задача при повторном запуске не выполняется заново, а догрузка за период
— обычный цикл по датам. Когда объём перерастёт этот подход, задачи переедут в Airflow как есть:
они обычные функции.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import date, timedelta

from cbr.store import BLOCKED, DONE, FAILED, SKIPPED, Store


class FlowError(ValueError):
    """Граф задач описан неверно."""


@dataclass
class Context:
    """Что доступно задаче: день, за который её запустили, хранилище и заметки соседних задач."""

    day: date
    store: Store
    notes: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class Task:
    name: str
    run: Callable[[Context], str]
    depends_on: tuple[str, ...] = ()
    retries: int = 2
    retry_delay: float = 2.0

    def pause(self, attempt: int) -> float:
        """Пауза перед следующей попыткой: 2, 4, 8 секунд."""
        return self.retry_delay * 2.0 ** max(attempt - 1, 0)


@dataclass(frozen=True, slots=True)
class TaskResult:
    task: str
    status: str
    attempts: int
    message: str
    seconds: float


@dataclass(frozen=True, slots=True)
class RunReport:
    day: date
    results: tuple[TaskResult, ...]

    @property
    def ok(self) -> bool:
        return all(result.status != FAILED for result in self.results)

    @property
    def failed(self) -> tuple[TaskResult, ...]:
        return tuple(result for result in self.results if result.status == FAILED)

    @property
    def line(self) -> str:
        parts = [f"{result.task}: {result.status}" for result in self.results]
        return f"{self.day:%d.%m.%Y} — " + ", ".join(parts)


class Dag:
    """Граф задач: проверяется при создании, чтобы ошибка в зависимостях не всплыла ночью."""

    def __init__(self, name: str, tasks: Sequence[Task]) -> None:
        if not tasks:
            raise FlowError("В графе должна быть хотя бы одна задача")
        names = [task.name for task in tasks]
        duplicates = {item for item in names if names.count(item) > 1}
        if duplicates:
            raise FlowError(f"Имена задач повторяются: {', '.join(sorted(duplicates))}")
        known = set(names)
        for task in tasks:
            unknown = set(task.depends_on) - known
            if unknown:
                raise FlowError(f"Задача «{task.name}» зависит от несуществующих: {', '.join(sorted(unknown))}")
        self.name = name
        self.tasks = {task.name: task for task in tasks}
        self.order = self._order(tasks)

    def _order(self, tasks: Sequence[Task]) -> tuple[Task, ...]:
        ordered: list[Task] = []
        placed: set[str] = set()
        left = list(tasks)
        while left:
            ready = [task for task in left if set(task.depends_on) <= placed]
            if not ready:
                stuck = ", ".join(sorted(task.name for task in left))
                raise FlowError(f"В зависимостях кольцо: {stuck}")
            for task in ready:
                ordered.append(task)
                placed.add(task.name)
            left = [task for task in left if task.name not in placed]
        return tuple(ordered)

    def run_for(
        self, day: date, store: Store, *, force: bool = False, sleep: Callable[[float], None] | None = None
    ) -> RunReport:
        """Выполняет граф за один день. Уже сделанная задача второй раз не выполняется."""
        wait = sleep or time.sleep
        context = Context(day=day, store=store)
        results: list[TaskResult] = []
        statuses: dict[str, str] = {}

        for task in self.order:
            # Задача выполняется, только если все её зависимости завершились успешно: упавшая
            # задача блокирует не только соседей, но и всё, что идёт за ними по цепочке.
            blocked = [name for name in task.depends_on if statuses.get(name) != DONE]
            if blocked:
                results.append(TaskResult(task.name, BLOCKED, 0, f"не выполнена: {', '.join(blocked)}", 0.0))
                statuses[task.name] = BLOCKED
                continue
            if not force and store.task_status(self.name, task.name, day) == DONE:
                results.append(TaskResult(task.name, SKIPPED, 0, "уже выполнялась в этот день", 0.0))
                statuses[task.name] = DONE
                continue
            result = self._attempt(task, context, wait)
            results.append(result)
            statuses[task.name] = result.status
        return RunReport(day=day, results=tuple(results))

    def _attempt(self, task: Task, context: Context, wait: Callable[[float], None]) -> TaskResult:
        message = ""
        started = time.monotonic()
        for attempt in range(1, task.retries + 2):
            try:
                message = task.run(context)
                seconds = time.monotonic() - started
                context.store.record_attempt(
                    dag=self.name,
                    task=task.name,
                    run_date=context.day,
                    attempt=attempt,
                    status=DONE,
                    message=message,
                    seconds=seconds,
                )
                context.notes[task.name] = message
                return TaskResult(task.name, DONE, attempt, message, seconds)
            # Задача может упасть как угодно — конвейер обязан уцелеть и записать причину.
            except Exception as exc:
                message = f"{type(exc).__name__}: {exc}"
                seconds = time.monotonic() - started
                context.store.record_attempt(
                    dag=self.name,
                    task=task.name,
                    run_date=context.day,
                    attempt=attempt,
                    status=FAILED if attempt > task.retries else "повтор",
                    message=message,
                    seconds=seconds,
                )
                if attempt <= task.retries:
                    wait(task.pause(attempt))
        return TaskResult(task.name, FAILED, task.retries + 1, message, time.monotonic() - started)

    def backfill(
        self,
        since: date,
        until: date,
        store: Store,
        *,
        force: bool = False,
        sleep: Callable[[float], None] | None = None,
    ) -> list[RunReport]:
        """Догрузка за период: тот же граф, просто по одному дню за раз."""
        if since > until:
            raise FlowError("Начало периода позже его конца")
        reports: list[RunReport] = []
        day = since
        while day <= until:
            reports.append(self.run_for(day, store, force=force, sleep=sleep))
            day += timedelta(days=1)
        return reports
