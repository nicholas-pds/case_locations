import os
import pandas as pd
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from dashboard.data.cache import cache
from dashboard.data.transforms import (
    aggregate_by_location, filter_cases, add_filter_columns,
)
from dashboard.config import CATEGORY_COLORS

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def main_page(request: Request):
    df = await cache.get("case_locations")
    metadata = await cache.get_metadata()

    if df is not None and not df.empty:
        if 'IsRush' not in df.columns:
            df = add_filter_columns(df)
        locations = aggregate_by_location(df)
        total_cases = len(df)
        all_locations = sorted(df['Last Location'].dropna().unique().tolist())
        all_categories = sorted(df['Category'].dropna().unique().tolist())
        cases = df.to_dict('records')

        # Add "No Location" if there are cases with null/blank locations
        null_mask = df['Last Location'].isna() | (df['Last Location'].astype(str).str.strip() == '')
        if null_mask.any():
            all_locations.append('No Location')

        # Build ship date options sorted oldest→newest, formatted as M/D
        _date_fmt = '%#m/%#d' if os.name == 'nt' else '%-m/%-d'
        ship_dates_raw = df['Ship Date'].dropna().unique()
        ship_dates_sorted = sorted(ship_dates_raw)
        all_ship_dates = []
        seen = set()
        for d in ship_dates_sorted:
            try:
                if hasattr(d, 'strftime'):
                    formatted = d.strftime(_date_fmt)
                else:
                    formatted = pd.to_datetime(d).strftime(_date_fmt)
                if formatted not in seen:
                    seen.add(formatted)
                    all_ship_dates.append(formatted)
            except Exception:
                pass
    else:
        locations = []
        total_cases = 0
        all_locations = []
        all_categories = []
        all_ship_dates = []
        cases = []

    templates = request.app.state.templates
    return templates.TemplateResponse("pages/main.html", {
        "request": request,
        "locations": locations,
        "cases": cases[:50],
        "total_cases": total_cases,
        "all_locations": all_locations,
        "all_categories": all_categories,
        "all_ship_dates": all_ship_dates,
        "metadata": metadata,
        "active_filter": None,
        "category_colors": CATEGORY_COLORS,
        "active_page": "main",
    })
