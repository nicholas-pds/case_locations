import logging
import pandas as pd
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from dashboard.data.cache import cache
from dashboard.data.transforms import (
    build_workload_chart_data,
    build_workload_pace_data,
    build_sales_history,
    aggregate_airway_stages,
    build_monthly_sales_chart,
    build_daily_sales_chart,
    build_monthly_goals_chart,
    build_annual_goals_chart,
)
from dashboard.data.revenue_goals_store import load_revenue_goals, save_revenue_goals
import json

logger = logging.getLogger("dashboard.daily_summary")

router = APIRouter()

_EMPTY_CHART = {'labels': [], 'data': [], 'trend': [], 'is_current': []}
_EMPTY_DAILY_CHART = {'labels': [], 'data': [], 'trend': [], 'is_today': []}
_EMPTY_GOALS_CHART = {
    'labels': [], 'goals': [], 'actuals': [], 'colors': [], 'pct_of_goals': [], 'year': ''
}
_EMPTY_ANNUAL_CHART = {
    'labels': [], 'goals': [], 'actuals': [], 'colors': [], 'pct_of_goals': []
}


@router.get("/daily-summary", response_class=HTMLResponse)
async def daily_summary_page(request: Request):
    metadata = await cache.get_metadata()

    # Submitted cases count
    submitted_df = await cache.get("submitted_cases")
    submitted_count = len(submitted_df) if submitted_df is not None and not submitted_df.empty else 0

    # Invoiced history (last 5 business days)
    history_df = await cache.get("daily_sales")
    sales_history = build_sales_history(history_df) if history_df is not None and not history_df.empty else []

    # Workload summary (reuse existing data)
    status_df = await cache.get("workload_status")
    chart_data = build_workload_chart_data(status_df) if status_df is not None and not status_df.empty else {
        'labels': [], 'invoiced': [], 'in_production': []
    }
    total_in_production = sum(chart_data['in_production'])
    pace_data = build_workload_pace_data(status_df) if status_df is not None else []

    # Airway planning count (same as airway workflow page badge)
    airway_df = await cache.get("airway_workflow")
    stages = aggregate_airway_stages(airway_df) if airway_df is not None else {}
    airway_planning_count = sum(s['total'] for group in stages.values() for s in group)

    # New charts: monthly sales + goals
    monthly_df = await cache.get("monthly_sales")
    goals_df = load_revenue_goals()

    monthly_chart = build_monthly_sales_chart(monthly_df) if monthly_df is not None else _EMPTY_CHART
    daily_chart = build_daily_sales_chart(history_df) if history_df is not None else _EMPTY_DAILY_CHART
    monthly_goals_chart = (
        build_monthly_goals_chart(monthly_df, goals_df) if monthly_df is not None
        else _EMPTY_GOALS_CHART
    )
    annual_goals_chart = build_annual_goals_chart(monthly_df, goals_df)

    templates = request.app.state.templates
    return templates.TemplateResponse("pages/daily_summary.html", {
        "request": request,
        "metadata": metadata,
        "active_page": "daily-summary",
        "submitted_count": submitted_count,
        "sales_history": sales_history,
        "sales_history_json": json.dumps(sales_history),
        "total_in_production": total_in_production,
        "airway_planning_count": airway_planning_count,
        "pace_data": pace_data,
        "workload_chart_json": json.dumps(chart_data),
        "monthly_chart_json": json.dumps(monthly_chart),
        "daily_chart_json": json.dumps(daily_chart),
        "monthly_goals_json": json.dumps(monthly_goals_chart),
        "annual_goals_json": json.dumps(annual_goals_chart),
        "monthly_goals_year": monthly_goals_chart.get("year", ""),
    })


# ─────────────────────────────────────────────
# Revenue Goals CRUD
# ─────────────────────────────────────────────

@router.get("/daily-summary/revenue-goals")
async def get_revenue_goals():
    """Return revenue goals as JSON array."""
    df = load_revenue_goals()
    records = df.to_dict(orient='records')
    return JSONResponse(content=records)


@router.post("/daily-summary/revenue-goals")
async def save_revenue_goals_endpoint(request: Request):
    """Save revenue goals from JSON array."""
    try:
        data = await request.json()
        df = pd.DataFrame(data)
        for col in ["Year", "Month", "RevenueGoal"]:
            if col not in df.columns:
                return JSONResponse(
                    content={"status": "error", "message": f"Missing column: {col}"},
                    status_code=400,
                )
        save_revenue_goals(df)
        return JSONResponse(content={"status": "ok", "rows": len(df)})
    except Exception as e:
        logger.error(f"Save revenue goals failed: {e}")
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=400)
