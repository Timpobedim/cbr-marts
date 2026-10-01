"""Витрины: дневная динамика, помесячные средние, изменения и кросс-курсы.

Витрины материализуются таблицами, а не живут запросами в SQL Lab внутри Superset. Витрина — часть
конвейера: её
пересобирает задача, и она попадает в отчёт и на страницу.
"""

from __future__ import annotations

from typing import Final

from cbr.store import Store

TRACKED: Final = ("USD", "EUR", "CNY", "GBP", "KZT", "JPY", "TRY", "CHF")
"""Валюты, которые показываются в отчёте. В хранилище лежат все, что отдаёт ЦБ."""

MARTS: Final = {
    "mart_rate_daily": """
        CREATE OR REPLACE TABLE mart_rate_daily AS
        SELECT f.rate_date,
               f.code,
               c.name,
               CAST(f.per_unit AS DOUBLE) AS per_unit,
               CAST(f.per_unit - lag(f.per_unit) OVER (PARTITION BY f.code ORDER BY f.rate_date) AS DOUBLE)
                   AS change_day
        FROM fact_rate f
        JOIN dim_currency c ON c.code = f.code
        ORDER BY f.code, f.rate_date
    """,
    "mart_month": """
        CREATE OR REPLACE TABLE mart_month AS
        SELECT code,
               year(rate_date) AS year,
               month(rate_date) AS month,
               count(*) AS days,
               CAST(avg(per_unit) AS DOUBLE) AS avg_rate,
               CAST(min(per_unit) AS DOUBLE) AS min_rate,
               CAST(max(per_unit) AS DOUBLE) AS max_rate,
               CAST((max(per_unit) - min(per_unit)) / nullif(min(per_unit), 0) AS DOUBLE) AS spread
        FROM fact_rate
        GROUP BY 1, 2, 3
        ORDER BY code, year, month
    """,
    # Курсы публикуются по рабочим дням, поэтому «неделю назад» нельзя искать по точной дате:
    # берётся последний курс на дату не позже нужной.
    "mart_change": """
        CREATE OR REPLACE TABLE mart_change AS
        WITH last_day AS (SELECT max(rate_date) AS day FROM fact_rate),
             points AS (
                 SELECT code,
                        arg_max(per_unit, rate_date) AS latest,
                        arg_max(per_unit, rate_date) FILTER (
                            WHERE rate_date < (SELECT day FROM last_day)) AS previous,
                        arg_max(per_unit, rate_date) FILTER (
                            WHERE rate_date <= (SELECT day FROM last_day) - 7) AS week_ago,
                        arg_max(per_unit, rate_date) FILTER (
                            WHERE rate_date <= (SELECT day FROM last_day) - 30) AS month_ago
                 FROM fact_rate GROUP BY code
             )
        SELECT p.code,
               c.name,
               (SELECT day FROM last_day) AS rate_date,
               CAST(p.latest AS DOUBLE) AS per_unit,
               CAST((p.latest - p.previous) / nullif(p.previous, 0) AS DOUBLE) AS change_day,
               CAST((p.latest - p.week_ago) / nullif(p.week_ago, 0) AS DOUBLE) AS change_week,
               CAST((p.latest - p.month_ago) / nullif(p.month_ago, 0) AS DOUBLE) AS change_month
        FROM points p JOIN dim_currency c ON c.code = p.code
        ORDER BY p.code
    """,
    "mart_cross": """
        CREATE OR REPLACE TABLE mart_cross AS
        SELECT eur.rate_date,
               CAST(eur.per_unit / usd.per_unit AS DOUBLE) AS eur_usd,
               CAST(cny.per_unit / usd.per_unit AS DOUBLE) AS cny_usd
        FROM fact_rate eur
        JOIN fact_rate usd ON usd.rate_date = eur.rate_date AND usd.code = 'USD'
        LEFT JOIN fact_rate cny ON cny.rate_date = eur.rate_date AND cny.code = 'CNY'
        WHERE eur.code = 'EUR'
        ORDER BY eur.rate_date
    """,
}


def build(store: Store) -> dict[str, int]:
    """Пересобирает все витрины и возвращает число строк в каждой."""
    sizes: dict[str, int] = {}
    for name, statement in MARTS.items():
        store.execute(statement)
        sizes[name] = store.count(name)
    return sizes
