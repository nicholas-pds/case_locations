from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from dashboard.data.cache import cache
from dashboard.data.transforms import filter_overdue_no_scan

router = APIRouter()


@router.get("/overdue-noscan", response_class=HTMLResponse)
async def overdue_noscan_page(request: Request):
    df = await cache.get("case_locations")
    metadata = await cache.get_metadata()

    if df is not None and not df.empty:
        df = filter_overdue_no_scan(df)
        total_cases = len(df)
        cases = df.to_dict('records')
    else:
        total_cases = 0
        cases = []

    templates = request.app.state.templates
    return templates.TemplateResponse("pages/overdue_noscan.html", {
        "request": request,
        "cases": cases,
        "total_cases": total_cases,
        "metadata": metadata,
        "active_page": "overdue-noscan",
    })
