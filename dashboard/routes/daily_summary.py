from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from dashboard.data.cache import cache
from dashboard.data.transforms import build_workload_chart_data, build_workload_pace_data, build_sales_history
import json

router = APIRouter()


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
    total_invoiced = sum(chart_data['invoiced'])
    pace_data = build_workload_pace_data(status_df) if status_df is not None else []

    templates = request.app.state.templates
    return templates.TemplateResponse("pages/daily_summary.html", {
        "request": request,
        "metadata": metadata,
        "active_page": "daily-summary",
        "submitted_count": submitted_count,
        "sales_history": sales_history,
        "sales_history_json": json.dumps(sales_history),
        "total_in_production": total_in_production,
        "total_invoiced": total_invoiced,
        "pace_data": pace_data,
    })
