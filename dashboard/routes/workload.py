from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from dashboard.data.cache import cache
from dashboard.data.transforms import build_workload_chart_data, build_workload_pivot_table, build_workload_pace_data, build_category_pace_data
import json

router = APIRouter()

# Locations for stage-based tiles
DESIGN_LOCATIONS = ['Design Cart', '3D Design']
MANUFACTURING_LOCATIONS = ['3D Manufacturing', 'Oven', 'Tumbler']
PRODUCTION_FLOOR_LOCATIONS = [
    'Metal Shelf', 'Metal Finish', 'Metal Polishing',
    'Banding', 'Metal Bending', 'Welding',
    'Marpe', 'Wire Bending',
    'Acrylic', 'Wire Finishing/Polishing', 'Essix Shelf',
    'QC', 'Production Floor Desk',
]


def _count_by_locations(df, locations):
    """Count cases where Last Location is in the given list."""
    if df is None or df.empty or 'Last Location' not in df.columns:
        return 0
    return int(df['Last Location'].isin(locations).sum())


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
    pace_data = build_workload_pace_data(status_df) if status_df is not None else []
    category_pace_data = build_category_pace_data(pivot_df) if pivot_df is not None else []

    total_in_production = sum(chart_data['in_production'])
    total_invoiced = sum(chart_data['invoiced'])

    # Stage tile counts
    submitted_df = await cache.get("submitted_cases")
    case_df = await cache.get("case_locations")
    submitted_count = len(submitted_df) if submitted_df is not None and not submitted_df.empty else 0
    design_count = _count_by_locations(case_df, DESIGN_LOCATIONS)
    manufacturing_count = _count_by_locations(case_df, MANUFACTURING_LOCATIONS)
    production_floor_count = _count_by_locations(case_df, PRODUCTION_FLOOR_LOCATIONS)

    templates = request.app.state.templates
    return templates.TemplateResponse("pages/workload.html", {
        "request": request,
        "chart_data": json.dumps(chart_data),
        "pivot_data": pivot_data,
        "pace_data": pace_data,
        "metadata": metadata,
        "active_page": "workload",
        "total_in_production": total_in_production,
        "total_invoiced": total_invoiced,
        "submitted_count": submitted_count,
        "design_count": design_count,
        "manufacturing_count": manufacturing_count,
        "production_floor_count": production_floor_count,
        "category_pace_data": category_pace_data,
    })
