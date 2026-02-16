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

    # Compute total across all stages
    total_cases = 0
    for group_stages in stages.values():
        for stage in group_stages:
            total_cases += stage['total']

    templates = request.app.state.templates
    return templates.TemplateResponse("pages/airway_workflow.html", {
        "request": request,
        "stages": stages,
        "total_cases": total_cases,
        "metadata": metadata,
        "active_page": "airway",
    })
