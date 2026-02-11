from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from dashboard.data.cache import cache
from dashboard.data.transforms import build_workload_chart_data, build_workload_pivot_table
import json

router = APIRouter()


@router.get("/workload", response_class=HTMLResponse)
async def workload_page(request: Request):
    status_df = await cache.get("workload_status")
    pivot_df = await cache.get("workload_pivot")
    metadata = await cache.get_metadata()

    chart_data = build_workload_chart_data(status_df) if status_df is not None else {
        'labels': [], 'invoiced': [], 'in_production': []
    }
    pivot_data = build_workload_pivot_table(pivot_df) if pivot_df is not None else {
        'dates': [], 'categories': [], 'data': {}, 'totals': {}
    }

    templates = request.app.state.templates
    return templates.TemplateResponse("pages/workload.html", {
        "request": request,
        "chart_data": json.dumps(chart_data),
        "pivot_data": pivot_data,
        "metadata": metadata,
        "active_page": "workload",
    })
