#!/usr/bin/env python3
"""Create the MVP Superset database, datasets, charts, and dashboard.

Run inside the Superset container:
    python /tmp/bootstrap_superset_dashboard.py

This script is intentionally local-MVP oriented. It uses Superset's internal
metadata models to avoid CSRF/API bootstrapping complexity.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from superset.app import create_app


DATABASE_NAME = os.environ.get("SUPERSET_STARROCKS_DATABASE_NAME", "StarRocks Hotel Booking")
SQLALCHEMY_URI = os.environ.get(
    "SUPERSET_STARROCKS_SQLALCHEMY_URI",
    "starrocks://root:@starrocks:9030/default_catalog.hotel_booking",
)
SCHEMA = os.environ.get("SUPERSET_STARROCKS_SCHEMA", "hotel_booking")
DASHBOARD_TITLE = os.environ.get("SUPERSET_DASHBOARD_TITLE", "Hotel Booking BI Dashboard")
DASHBOARD_SLUG = os.environ.get("SUPERSET_DASHBOARD_SLUG", "hotel-booking-bi-dashboard")


MART_TABLES = [
    "mart_daily_booking_revenue",
    "mart_monthly_booking_revenue",
    "mart_hotel_performance",
    "mart_room_performance",
    "mart_market_segment_performance",
    "mart_channel_performance",
    "mart_country_performance",
    "mart_cancellation_analysis",
    "mart_lead_time_analysis",
    "mart_customer_type_performance",
]


@dataclass(frozen=True)
class ChartSpec:
    name: str
    viz_type: str
    table: str
    params: dict[str, Any]
    width: int = 4
    height: int = 12


def adhoc_metric(label: str, expression: str) -> dict[str, str]:
    option = (
        "metric_"
        + "".join(ch.lower() if ch.isalnum() else "_" for ch in label).strip("_")
    )
    return {
        "expressionType": "SQL",
        "sqlExpression": expression,
        "label": label,
        "optionName": option,
    }


def big_number_params(metric_label: str, expression: str, currency: bool = False) -> dict[str, Any]:
    return {
        "viz_type": "big_number_total",
        "metric": adhoc_metric(metric_label, expression),
        "subheader": "",
        "y_axis_format": "$,.3s" if currency else ",.3s",
    }


def line_params(
    time_col: str,
    metric_label: str,
    expression: str,
    y_axis_format: str = ",.3s",
) -> dict[str, Any]:
    return {
        "viz_type": "echarts_timeseries_line",
        "x_axis": time_col,
        "time_grain_sqla": "P1D",
        "granularity_sqla": time_col,
        "metrics": [adhoc_metric(metric_label, expression)],
        "groupby": [],
        "adhoc_filters": [],
        "row_limit": 10000,
        "order_desc": True,
        "show_legend": True,
        "y_axis_format": y_axis_format,
    }


def bar_params(
    dimension: str,
    metric_label: str,
    expression: str,
    y_axis_format: str = ",.3s",
    row_limit: int = 10,
) -> dict[str, Any]:
    return {
        "viz_type": "echarts_timeseries_bar",
        "x_axis": dimension,
        "metrics": [adhoc_metric(metric_label, expression)],
        "groupby": [],
        "adhoc_filters": [],
        "row_limit": row_limit,
        "order_desc": True,
        "y_axis_format": y_axis_format,
        "show_legend": True,
        "orientation": "vertical",
    }


def table_params(
    columns: list[str],
    metrics: list[tuple[str, str]],
    row_limit: int = 50,
) -> dict[str, Any]:
    return {
        "viz_type": "table",
        "query_mode": "aggregate",
        "groupby": columns,
        "metrics": [adhoc_metric(label, expression) for label, expression in metrics],
        "all_columns": [],
        "adhoc_filters": [],
        "row_limit": row_limit,
        "server_page_length": 10,
        "order_desc": True,
        "show_cell_bars": True,
    }


CHARTS = [
    ChartSpec(
        "Total bookings",
        "big_number_total",
        "mart_hotel_performance",
        big_number_params("Total bookings", "SUM(bookings)"),
        width=3,
        height=28,
    ),
    ChartSpec(
        "Cancellation rate",
        "big_number_total",
        "mart_hotel_performance",
        big_number_params(
            "Cancellation rate",
            "SUM(cancelled_bookings) / NULLIF(SUM(bookings), 0)",
        )
        | {"y_axis_format": ".2%"},
        width=3,
        height=28,
    ),
    ChartSpec(
        "Realized revenue",
        "big_number_total",
        "mart_hotel_performance",
        big_number_params("Realized revenue", "SUM(realized_revenue)", currency=True),
        width=3,
        height=28,
    ),
    ChartSpec(
        "Average ADR",
        "big_number_total",
        "mart_hotel_performance",
        big_number_params("Average ADR", "AVG(avg_adr)", currency=True),
        width=3,
        height=28,
    ),
    ChartSpec(
        "Daily bookings trend",
        "echarts_timeseries_line",
        "mart_daily_booking_revenue",
        line_params("arrival_date", "Bookings", "SUM(total_bookings)"),
        width=6,
        height=52,
    ),
    ChartSpec(
        "Monthly estimated revenue trend",
        "echarts_timeseries_line",
        "mart_monthly_booking_revenue",
        line_params("month_start_date", "Estimated revenue", "SUM(estimated_revenue)", "$,.3s"),
        width=6,
        height=52,
    ),
    ChartSpec(
        "Revenue by hotel type",
        "echarts_timeseries_bar",
        "mart_hotel_performance",
        bar_params("hotel", "Realized revenue", "SUM(realized_revenue)", "$,.3s"),
        width=6,
        height=50,
    ),
    ChartSpec(
        "Revenue by room type",
        "echarts_timeseries_bar",
        "mart_room_performance",
        bar_params("reserved_room_type", "Realized revenue", "SUM(realized_revenue)", "$,.3s"),
        width=6,
        height=50,
    ),
    ChartSpec(
        "Revenue by market segment",
        "echarts_timeseries_bar",
        "mart_market_segment_performance",
        bar_params("market_segment", "Realized revenue", "SUM(realized_revenue)", "$,.3s"),
        width=6,
        height=50,
    ),
    ChartSpec(
        "Cancellation rate by distribution channel",
        "echarts_timeseries_bar",
        "mart_channel_performance",
        bar_params(
            "distribution_channel",
            "Cancellation rate",
            "SUM(cancelled_bookings) / NULLIF(SUM(bookings), 0)",
            ".2%",
        ),
        width=6,
        height=50,
    ),
    ChartSpec(
        "Top countries by bookings",
        "echarts_timeseries_bar",
        "mart_country_performance",
        bar_params("country", "Bookings", "SUM(bookings)", ",.3s", row_limit=10),
        width=6,
        height=50,
    ),
    ChartSpec(
        "Cancellation rate by lead time bucket",
        "echarts_timeseries_bar",
        "mart_lead_time_analysis",
        bar_params(
            "lead_time_bucket",
            "Cancellation rate",
            "SUM(cancelled_bookings) / NULLIF(SUM(bookings), 0)",
            ".2%",
        ),
        width=6,
        height=50,
    ),
    ChartSpec(
        "Customer type performance",
        "table",
        "mart_customer_type_performance",
        table_params(
            ["customer_type", "guest_type"],
            [
                ("Bookings", "SUM(bookings)"),
                ("Realized revenue", "SUM(realized_revenue)"),
                ("Cancellation rate", "SUM(cancelled_bookings) / NULLIF(SUM(bookings), 0)"),
            ],
        ),
        width=12,
        height=60,
    ),
]


def json_dumps(value: dict[str, Any]) -> str:
    return json.dumps(value, sort_keys=True)


def get_or_create_database(db_session: Any) -> Any:
    from superset.models.core import Database

    database = (
        db_session.query(Database)
        .filter(Database.database_name == DATABASE_NAME)
        .one_or_none()
    )
    if database is None:
        database = Database(database_name=DATABASE_NAME, expose_in_sqllab=True)
        db_session.add(database)

    database.set_sqlalchemy_uri(SQLALCHEMY_URI)
    database.expose_in_sqllab = True
    database.allow_ctas = False
    database.allow_cvas = False
    database.allow_dml = False
    database.extra = json_dumps({"metadata_params": {}, "engine_params": {}})
    db_session.commit()
    return database


def get_or_create_dataset(db_session: Any, database: Any, table_name: str) -> Any:
    from superset.connectors.sqla.models import SqlaTable

    dataset = (
        db_session.query(SqlaTable)
        .filter(
            SqlaTable.database_id == database.id,
            SqlaTable.schema == SCHEMA,
            SqlaTable.table_name == table_name,
        )
        .one_or_none()
    )
    if dataset is None:
        dataset = SqlaTable(
            table_name=table_name,
            schema=SCHEMA,
            database=database,
            database_id=database.id,
        )
        db_session.add(dataset)
        db_session.flush()

    dataset.table_name = table_name
    dataset.schema = SCHEMA
    dataset.database = database
    dataset.database_id = database.id
    dataset.is_sqllab_view = False
    dataset.filter_select_enabled = True
    dataset.fetch_metadata()
    db_session.commit()
    return dataset


def build_query_context(chart: ChartSpec, dataset: Any) -> str:
    form_data = {"datasource": f"{dataset.id}__table", **chart.params}
    metrics = chart.params.get("metrics")
    if metrics is None and "metric" in chart.params:
        metrics = [chart.params["metric"]]
    if metrics is None:
        metrics = []

    query_context = {
        "datasource": {"id": dataset.id, "type": "table"},
        "force": False,
        "queries": [
            {
                "filters": [],
                "extras": {"having": "", "where": ""},
                "applied_time_extras": {},
                "columns": chart.params.get("groupby", []),
                "metrics": metrics,
                "orderby": [],
                "annotation_layers": [],
                "row_limit": chart.params.get("row_limit", 10000),
                "series_limit": 0,
                "order_desc": chart.params.get("order_desc", True),
                "url_params": {},
                "custom_params": {},
                "custom_form_data": {},
                "time_offsets": [],
            }
        ],
        "form_data": form_data,
        "result_format": "json",
        "result_type": "full",
    }
    return json_dumps(query_context)


def get_or_create_chart(db_session: Any, chart: ChartSpec, dataset: Any) -> Any:
    from superset.models.slice import Slice

    with db_session.no_autoflush:
        slice_obj = (
            db_session.query(Slice)
            .filter(Slice.slice_name == chart.name)
            .one_or_none()
        )
        if slice_obj is None:
            slice_obj = Slice(slice_name=chart.name)

    slice_obj.slice_name = chart.name
    slice_obj.viz_type = chart.viz_type
    slice_obj.datasource_id = dataset.id
    slice_obj.datasource_type = "table"
    slice_obj.datasource_name = dataset.table_name
    params = {"datasource": f"{dataset.id}__table", **chart.params}
    slice_obj.params = json_dumps(params)
    slice_obj.query_context = build_query_context(chart, dataset)
    slice_obj.cache_timeout = None
    slice_obj.description = f"Auto-created for {DASHBOARD_TITLE}."
    db_session.add(slice_obj)
    db_session.commit()
    return slice_obj


def chart_component(chart_id: int, width: int, height: int) -> tuple[str, dict[str, Any]]:
    component_id = f"CHART-{chart_id}"
    return component_id, {
        "type": "CHART",
        "id": component_id,
        "children": [],
        "meta": {
            "chartId": chart_id,
            "height": height,
            "width": width,
            "uuid": "",
        },
    }


def row_component(index: int, children: list[str]) -> tuple[str, dict[str, Any]]:
    component_id = f"ROW-{index}"
    return component_id, {
        "type": "ROW",
        "id": component_id,
        "children": children,
        "meta": {"background": "BACKGROUND_TRANSPARENT"},
    }


def header_component(text: str, index: int) -> tuple[str, dict[str, Any]]:
    component_id = f"HEADER-{index}"
    return component_id, {
        "type": "HEADER",
        "id": component_id,
        "children": [],
        "meta": {"text": text, "background": "BACKGROUND_TRANSPARENT"},
    }


def build_position_json(charts: list[tuple[Any, ChartSpec]]) -> str:
    position: dict[str, Any] = {
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": []},
    }

    section_index = 1
    row_index = 1
    sections = [
        ("Overview KPIs", charts[0:4]),
        ("Time Trends", charts[4:6]),
        ("Hotel / Room Performance", charts[6:8]),
        ("Segment / Channel Analysis", charts[8:10]),
        ("Country / Lead Time", charts[10:12]),
        ("Customer Type", charts[12:13]),
    ]

    for section_title, section_charts in sections:
        header_id, header = header_component(section_title, section_index)
        position[header_id] = header
        position["GRID_ID"]["children"].append(header_id)
        section_index += 1

        current_row_children: list[str] = []
        current_row_width = 0
        for slice_obj, spec in section_charts:
            if current_row_children and current_row_width + spec.width > 12:
                row_id, row = row_component(row_index, current_row_children)
                position[row_id] = row
                position["GRID_ID"]["children"].append(row_id)
                row_index += 1
                current_row_children = []
                current_row_width = 0

            component_id, component = chart_component(slice_obj.id, spec.width, spec.height)
            position[component_id] = component
            current_row_children.append(component_id)
            current_row_width += spec.width

        if current_row_children:
            row_id, row = row_component(row_index, current_row_children)
            position[row_id] = row
            position["GRID_ID"]["children"].append(row_id)
            row_index += 1

    return json_dumps(position)


def get_or_create_dashboard(db_session: Any, charts: list[tuple[Any, ChartSpec]]) -> Any:
    from superset.models.dashboard import Dashboard

    dashboard = (
        db_session.query(Dashboard)
        .filter(Dashboard.dashboard_title == DASHBOARD_TITLE)
        .one_or_none()
    )
    if dashboard is None:
        dashboard = Dashboard(dashboard_title=DASHBOARD_TITLE, slug=DASHBOARD_SLUG)
        db_session.add(dashboard)

    dashboard.dashboard_title = DASHBOARD_TITLE
    dashboard.slug = DASHBOARD_SLUG
    dashboard.published = True
    dashboard.position_json = build_position_json(charts)
    dashboard.json_metadata = json_dumps(
        {
            "label_colors": {},
            "timed_refresh_immune_slices": [],
            "expanded_slices": {},
            "refresh_frequency": 0,
            "default_filters": "{}",
            "color_namespace": None,
            "show_native_filters": True,
        }
    )
    dashboard.description = (
        "Auto-created MVP dashboard. Charts use StarRocks mart tables only."
    )
    dashboard.slices = [slice_obj for slice_obj, _ in charts]
    db_session.commit()
    return dashboard


def main() -> None:
    app = create_app()
    with app.app_context():
        from superset.extensions import db

        database = get_or_create_database(db.session)
        datasets = {
            table_name: get_or_create_dataset(db.session, database, table_name)
            for table_name in MART_TABLES
        }
        created_charts = [
            (get_or_create_chart(db.session, chart, datasets[chart.table]), chart)
            for chart in CHARTS
        ]
        dashboard = get_or_create_dashboard(db.session, created_charts)

        print(f"Superset bootstrap completed at {datetime.utcnow().isoformat()}Z")
        print(f"Database: {database.database_name} (id={database.id})")
        print(f"Datasets: {len(datasets)}")
        print(f"Charts: {len(created_charts)}")
        print(f"Dashboard: {dashboard.dashboard_title} (id={dashboard.id}, slug={dashboard.slug})")


if __name__ == "__main__":
    main()
