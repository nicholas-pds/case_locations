import os
from datetime import date
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from dashboard.data.cache import cache
from dashboard.data.transforms import filter_local_delivery_by_date, add_filter_columns

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from src.holidays import previous_business_day, next_x_business_days

# Windows date format
_DATE_LABEL_FMT = '%A, %B %#d' if os.name == 'nt' else '%A, %B %-d'
_DATE_SHORT_FMT = '%#m/%#d' if os.name == 'nt' else '%-m/%-d'

router = APIRouter()


@router.get("/local-delivery", response_class=HTMLResponse)
async def local_delivery_page(request: Request, date_str: str = None):
    df = await cache.get("case_locations")
    metadata = await cache.get_metadata()

    # Parse the selected date, default to today
    today = date.today()
    if date_str:
        try:
            selected_date = date.fromisoformat(date_str)
        except ValueError:
            selected_date = today
    else:
        selected_date = today

    # Compute prev/next business days
    prev_date = previous_business_day(selected_date)
    next_date = next_x_business_days(selected_date, x_days_ahead=1)

    if df is not None and not df.empty:
        df = filter_local_delivery_by_date(df, selected_date)
        if not df.empty and 'IsRush' not in df.columns:
            df = add_filter_columns(df)
        total_cases = len(df)
        cases = df.to_dict('records')
    else:
        total_cases = 0
        cases = []

    # Date labels
    is_today = selected_date == today
    selected_label = "Today" if is_today else selected_date.strftime(_DATE_LABEL_FMT)
    selected_date_display = selected_date.strftime(_DATE_SHORT_FMT)

    templates = request.app.state.templates
    return templates.TemplateResponse("pages/local_delivery.html", {
        "request": request,
        "cases": cases,
        "total_cases": total_cases,
        "metadata": metadata,
        "active_page": "local-delivery",
        "selected_date": selected_date.isoformat(),
        "selected_label": selected_label,
        "selected_date_display": selected_date_display,
        "is_today": is_today,
        "prev_date": prev_date.isoformat(),
        "prev_date_label": prev_date.strftime(_DATE_SHORT_FMT),
        "next_date": next_date.isoformat(),
        "next_date_label": next_date.strftime(_DATE_SHORT_FMT),
    })
