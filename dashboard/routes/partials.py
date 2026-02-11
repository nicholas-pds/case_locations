"""HTMX partial endpoints for filtered tables and grids."""
from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse
from dashboard.data.cache import cache
from dashboard.data.transforms import (
    aggregate_by_location, filter_cases, add_filter_columns,
    filter_local_delivery, filter_local_delivery_today, filter_overdue_no_scan,
    build_workload_chart_data, build_workload_pivot_table,
    aggregate_airway_stages,
)
from dashboard.config import CATEGORY_COLORS
import json

router = APIRouter(prefix="/partials")


@router.get("/location-grid", response_class=HTMLResponse)
async def location_grid(
    request: Request,
    filter: str = None,
    search: str = None,
    location: str = None,
    category: str = None,
):
    df = await cache.get("case_locations")
    if df is not None and not df.empty:
        df = filter_cases(df, filter, search, location, category)
        locations = aggregate_by_location(df)
    else:
        locations = []

    templates = request.app.state.templates
    return templates.TemplateResponse("partials/location_grid.html", {
        "request": request,
        "locations": locations,
        "category_colors": CATEGORY_COLORS,
    })


@router.get("/case-table", response_class=HTMLResponse)
async def case_table(
    request: Request,
    filter: str = None,
    search: str = None,
    location: str = None,
    category: str = None,
    page: int = 1,
    page_size: int = 50,
):
    df = await cache.get("case_locations")
    if df is not None and not df.empty:
        df = filter_cases(df, filter, search, location, category)
        total_cases = len(df)
        start = (page - 1) * page_size
        end = start + page_size
        cases = df.iloc[start:end].to_dict('records')
    else:
        total_cases = 0
        cases = []

    templates = request.app.state.templates
    return templates.TemplateResponse("partials/case_table.html", {
        "request": request,
        "cases": cases,
        "total_cases": total_cases,
        "page": page,
        "page_size": page_size,
        "active_filter": filter,
    })


@router.get("/metadata", response_class=HTMLResponse)
async def metadata_badge(request: Request):
    metadata = await cache.get_metadata()
    df = await cache.get("case_locations")
    total_cases = len(df) if df is not None else 0

    templates = request.app.state.templates
    return templates.TemplateResponse("partials/metadata_badge.html", {
        "request": request,
        "metadata": metadata,
        "total_cases": total_cases,
    })


@router.get("/workload-chart-data")
async def workload_chart_data():
    df = await cache.get("workload_status")
    chart_data = build_workload_chart_data(df) if df is not None else {
        'labels': [], 'invoiced': [], 'in_production': []
    }
    return chart_data


@router.get("/workload-table", response_class=HTMLResponse)
async def workload_table(request: Request):
    df = await cache.get("workload_pivot")
    pivot_data = build_workload_pivot_table(df) if df is not None else {
        'dates': [], 'categories': [], 'data': {}, 'totals': {}
    }
    templates = request.app.state.templates
    return templates.TemplateResponse("partials/workload_table.html", {
        "request": request,
        "pivot_data": pivot_data,
    })


@router.get("/airway-grid", response_class=HTMLResponse)
async def airway_grid(request: Request):
    df = await cache.get("airway_workflow")
    stages = aggregate_airway_stages(df) if df is not None else {}
    templates = request.app.state.templates
    return templates.TemplateResponse("partials/airway_grid.html", {
        "request": request,
        "stages": stages,
    })


@router.get("/airway-table", response_class=HTMLResponse)
async def airway_table(request: Request, location: str = None):
    df = await cache.get("airway_workflow")
    if df is not None and not df.empty:
        if location:
            df = df[df['LastLocation'] == location]
        cases = df.to_dict('records')[:100]
    else:
        cases = []
    templates = request.app.state.templates
    return templates.TemplateResponse("partials/airway_table.html", {
        "request": request,
        "cases": cases,
    })


@router.get("/airway-hold-table", response_class=HTMLResponse)
async def airway_hold_table(request: Request, hold_status: str = None):
    df = await cache.get("airway_hold_status")
    if df is not None and not df.empty:
        if hold_status:
            df = df[df['HoldStatus'] == hold_status]
        cases = df.to_dict('records')
    else:
        cases = []
    templates = request.app.state.templates
    return templates.TemplateResponse("partials/airway_hold_table.html", {
        "request": request,
        "cases": cases,
    })


@router.get("/local-delivery-table", response_class=HTMLResponse)
async def local_delivery_table(request: Request):
    df = await cache.get("case_locations")
    if df is not None and not df.empty:
        df = filter_local_delivery_today(df)
        if not df.empty and 'IsRush' not in df.columns:
            df = add_filter_columns(df)
        cases = df.to_dict('records')
        total_cases = len(cases)
    else:
        cases = []
        total_cases = 0
    templates = request.app.state.templates
    return templates.TemplateResponse("partials/local_delivery_table.html", {
        "request": request,
        "cases": cases,
        "total_cases": total_cases,
    })


@router.get("/overdue-table", response_class=HTMLResponse)
async def overdue_table(request: Request):
    df = await cache.get("case_locations")
    if df is not None and not df.empty:
        df = filter_overdue_no_scan(df)
        cases = df.to_dict('records')
        total_cases = len(cases)
    else:
        cases = []
        total_cases = 0
    templates = request.app.state.templates
    return templates.TemplateResponse("partials/overdue_table.html", {
        "request": request,
        "cases": cases,
        "total_cases": total_cases,
    })
