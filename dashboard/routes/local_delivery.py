from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from dashboard.data.cache import cache
from dashboard.data.transforms import filter_local_delivery, add_filter_columns

router = APIRouter()


@router.get("/local-delivery", response_class=HTMLResponse)
async def local_delivery_page(request: Request):
    df = await cache.get("case_locations")
    metadata = await cache.get_metadata()

    if df is not None and not df.empty:
        df = filter_local_delivery(df)
        if not df.empty and 'IsRush' not in df.columns:
            df = add_filter_columns(df)
        total_cases = len(df)
        cases = df.to_dict('records')
    else:
        total_cases = 0
        cases = []

    templates = request.app.state.templates
    return templates.TemplateResponse("pages/local_delivery.html", {
        "request": request,
        "cases": cases,
        "total_cases": total_cases,
        "metadata": metadata,
        "active_page": "local-delivery",
    })
