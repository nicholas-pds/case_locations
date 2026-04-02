from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from dashboard.data.cache import cache
from dashboard.data.transforms import build_workload_chart_data, build_workload_pivot_table, build_workload_pace_data, build_category_pace_data
from src.holidays import previous_business_day, next_x_business_days, get_all_company_holidays
from datetime import date, datetime
import json
import pandas as pd

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
    denom = total_invoiced + total_in_production
    invoice_pace_pct = round(total_invoiced / denom * 100) if denom > 0 else 0

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
        "invoice_pace_pct": invoice_pace_pct,
        "submitted_count": submitted_count,
        "design_count": design_count,
        "manufacturing_count": manufacturing_count,
        "production_floor_count": production_floor_count,
        "category_pace_data": category_pace_data,
    })


def _df_to_gemba_records(df):
    """Convert a filtered case_locations DataFrame to JSON-safe gemba records."""
    records = []
    if df is None or df.empty:
        return records
    for _, row in df.iterrows():
        pan = str(row.get('Pan Number', '') or '')
        is_rush = pan.startswith('R') and len(pan) < 4
        ship = row.get('Ship Date')
        ship_str = ship.strftime('%Y-%m-%d') if hasattr(ship, 'strftime') else str(ship)
        records.append({
            'case_number': str(row.get('Case Number', '') or ''),
            'pan_number': pan,
            'ship_date': ship_str,
            'category': str(row.get('Category', '') or ''),
            'is_rush': is_rush,
        })
    return records


@router.get("/workload/3d-gemba-data")
async def gemba_data():
    case_df = await cache.get("case_locations")

    holidays = get_all_company_holidays()
    today = date.today()
    prev_biz = previous_business_day(holidays=holidays)
    next1 = next_x_business_days(today, 1, holidays=holidays)
    next2 = next_x_business_days(today, 2, holidays=holidays)
    dates = [
        prev_biz.strftime('%Y-%m-%d'),
        today.strftime('%Y-%m-%d'),
        next1.strftime('%Y-%m-%d'),
        next2.strftime('%Y-%m-%d'),
    ]

    def filter_loc(loc):
        if case_df is None or case_df.empty or 'Last Location' not in case_df.columns:
            return None
        return case_df[case_df['Last Location'] == loc]

    return JSONResponse({
        'dates': dates,
        'manufacturing_cases': _df_to_gemba_records(filter_loc('3D Manufacturing')),
        'oven_cases': _df_to_gemba_records(filter_loc('Oven')),
        'tumbler_cases': _df_to_gemba_records(filter_loc('Tumbler')),
    })


@router.get("/workload/pace-cases")
async def pace_cases(date_str: str = None, category: str = None):
    """Return individual In Production cases for a given (rush-adjusted) date and optional category.
    Uses workload_pivot_detail cache (same source as pace tiles) for consistent counts."""
    if not date_str:
        return JSONResponse({'cases': [], 'count': 0})

    detail_df = await cache.get("workload_pivot_detail")
    if detail_df is None or detail_df.empty:
        return JSONResponse({'cases': [], 'count': 0})

    # Filter to In Production only (pivot detail includes Invoiced too)
    filtered = detail_df[detail_df['Status'] == 'In Production'].copy()

    # Filter by rush-adjusted ShipDate (already adjusted in the cache)
    target = datetime.strptime(date_str, '%Y-%m-%d').date()
    mask = filtered['ShipDate'] == target
    if category:
        mask = mask & (filtered['Category'] == category)
    filtered = filtered[mask]

    # Sort by ShipDate ASC, then Category ASC
    filtered = filtered.sort_values(['ShipDate', 'Category'])

    cases = []
    for _, row in filtered.iterrows():
        ship = row.get('ShipDate')
        due = row.get('DueDate')
        cases.append({
            'ship_date': ship.strftime('%m/%d') if hasattr(ship, 'strftime') else str(ship),
            'due_date': due.strftime('%m/%d') if hasattr(due, 'strftime') and pd.notna(due) else '',
            'case_number': str(row.get('CaseNumber', '') or ''),
            'pan_number': str(row.get('PanNumber', '') or ''),
            'category': str(row.get('Category', '') or ''),
            'last_location': str(row.get('LastLocation', '') or ''),
            'local_delivery': bool(row.get('LocalDelivery', False)),
        })

    return JSONResponse({'cases': cases, 'count': len(cases)})
