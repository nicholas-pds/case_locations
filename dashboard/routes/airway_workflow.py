from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from dashboard.data.cache import cache
from dashboard.data.transforms import aggregate_airway_stages

router = APIRouter()


@router.get("/airway", response_class=HTMLResponse)
async def airway_workflow_page(request: Request):
    df = await cache.get("airway_workflow")
    metadata = await cache.get_metadata()

    stages = aggregate_airway_stages(df) if df is not None else {}
    cases = df.to_dict('records')[:100] if df is not None and not df.empty else []

    # Get unique values for filters
    all_locations = []
    if df is not None and not df.empty and 'LastLocation' in df.columns:
        all_locations = sorted(df['LastLocation'].dropna().unique().tolist())

    templates = request.app.state.templates
    return templates.TemplateResponse("pages/airway_workflow.html", {
        "request": request,
        "stages": stages,
        "cases": cases,
        "all_locations": all_locations,
        "metadata": metadata,
        "active_page": "airway",
    })
