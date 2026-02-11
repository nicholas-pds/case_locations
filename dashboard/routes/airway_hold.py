from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from dashboard.data.cache import cache

router = APIRouter()


@router.get("/airway-hold", response_class=HTMLResponse)
async def airway_hold_page(request: Request):
    df = await cache.get("airway_hold_status")
    metadata = await cache.get_metadata()

    cases = df.to_dict('records') if df is not None and not df.empty else []

    # Get unique values for filters
    hold_statuses = []
    if df is not None and not df.empty and 'HoldStatus' in df.columns:
        hold_statuses = sorted(df['HoldStatus'].dropna().unique().tolist())

    templates = request.app.state.templates
    return templates.TemplateResponse("pages/airway_hold.html", {
        "request": request,
        "cases": cases,
        "hold_statuses": hold_statuses,
        "metadata": metadata,
        "active_page": "airway-hold",
    })
