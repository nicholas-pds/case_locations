"""Business logic transforms: rush, overdue, leaves-today, aggregations."""
import os
from datetime import datetime, date, timedelta
import pandas as pd

# Windows uses %#m instead of %-m for non-zero-padded month/day
_DATE_FMT = '%#m/%#d' if os.name == 'nt' else '%-m/%-d'

import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.holidays import previous_business_day, get_all_company_holidays
from dashboard.config import MARPE_EXCLUDED_LOCATIONS, LOCATION_DISPLAY_ORDER, AIRWAY_STAGE_GROUPS


def is_rush(pan_number) -> bool:
    """Rush = PanNumber starts with 'R' and length < 4."""
    if pd.isna(pan_number):
        return False
    pan = str(pan_number).strip()
    return pan.upper().startswith('R') and len(pan) < 4


def is_leaves_today(ship_date, today: date = None) -> bool:
    """Leaves Today = ShipDate equals today."""
    if pd.isna(ship_date):
        return False
    if today is None:
        today = date.today()
    if hasattr(ship_date, 'date'):
        ship_date = ship_date.date()
    return ship_date == today


def is_overdue(ship_date, prev_biz_day: date = None) -> bool:
    """Overdue = ShipDate equals the previous business day."""
    if pd.isna(ship_date):
        return False
    if prev_biz_day is None:
        prev_biz_day = previous_business_day()
    if hasattr(ship_date, 'date'):
        ship_date = ship_date.date()
    return ship_date == prev_biz_day


def add_filter_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add Rush, LeavesToday, Overdue boolean columns to the DataFrame.

    Overdue includes rush cases with ShipDate == today (same-day urgent).
    """
    if df.empty:
        return df
    df = df.copy()
    prev_biz_day = previous_business_day()
    today = date.today()
    df['IsRush'] = df['Pan Number'].apply(is_rush)
    df['LeavesToday'] = df['Ship Date'].apply(lambda x: is_leaves_today(x, today))
    df['IsOverdue'] = df['Ship Date'].apply(lambda x: is_overdue(x, prev_biz_day))
    # Rush cases with ShipDate == today are also considered overdue
    rush_today = df['IsRush'] & df['LeavesToday']
    df['IsOverdue'] = df['IsOverdue'] | rush_today
    return df


def filter_cases(df: pd.DataFrame, filter_type: str = None,
                 search: str = None, location: str = None,
                 category: str = None) -> pd.DataFrame:
    """Apply filters to case location data."""
    if df.empty:
        return df

    result = df.copy()

    # Ensure filter columns exist
    if 'IsRush' not in result.columns:
        result = add_filter_columns(result)

    if filter_type == 'rush':
        result = result[result['IsRush']]
    elif filter_type == 'overdue':
        result = result[result['IsOverdue']]
    elif filter_type == 'leaves_today':
        result = result[result['LeavesToday']]

    if search:
        search = search.strip().upper()
        mask = (
            result['Case Number'].astype(str).str.upper().str.contains(search, na=False) |
            result['Pan Number'].astype(str).str.upper().str.contains(search, na=False)
        )
        result = result[mask]

    if location:
        result = result[result['Last Location'] == location]

    if category:
        result = result[result['Category'] == category]

    return result


def aggregate_by_location(df: pd.DataFrame) -> list[dict]:
    """Group cases by Last Location with category breakdowns.

    Returns a list of dicts sorted by LOCATION_DISPLAY_ORDER, with any
    extra locations appended at the end.
    """
    if df.empty:
        return []

    result = []
    grouped = df.groupby('Last Location')

    # Build lookup
    location_data = {}
    for loc_name, group in grouped:
        cats = group['Category'].value_counts().to_dict()
        total = len(group)
        location_data[loc_name] = {
            'name': loc_name,
            'total': total,
            'categories': cats,
        }

    # Only include locations in the display order
    for loc in LOCATION_DISPLAY_ORDER:
        if loc in location_data:
            result.append(location_data[loc])

    return result


def aggregate_airway_stages(df: pd.DataFrame) -> dict:
    """Group airway cases by workflow stage with date breakdowns.

    Returns dict grouped by section (NEW CASES, EMAIL, ZOOM).
    """
    if df.empty:
        return {group: [] for group in AIRWAY_STAGE_GROUPS}

    result = {}
    for group_name, locations in AIRWAY_STAGE_GROUPS.items():
        stages = []
        for loc in locations:
            stage_df = df[df['LastLocation'] == loc]
            total = len(stage_df)
            date_col = 'ShipDate' if 'ShipDate' in stage_df.columns else 'Ship Date'
            by_date = {}
            if not stage_df.empty and date_col in stage_df.columns:
                for d, count in stage_df[date_col].value_counts().sort_index().items():
                    if pd.notna(d):
                        date_str = d.strftime(_DATE_FMT) if hasattr(d, 'strftime') else str(d)
                        iso_str = d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else str(d)
                        by_date[date_str] = {'count': int(count), 'iso': iso_str}
            stages.append({
                'name': loc,
                'total': total,
                'by_date': dict(list(by_date.items())[:6]),
                'extra_dates': max(0, len(by_date) - 6),
            })
        result[group_name] = stages

    return result


def filter_local_delivery(df: pd.DataFrame) -> pd.DataFrame:
    """Filter to local delivery cases only."""
    if df.empty or 'LocalDelivery' not in df.columns:
        return df
    return df[df['LocalDelivery'] == True].copy()


def filter_local_delivery_today(df: pd.DataFrame) -> pd.DataFrame:
    """Filter to local delivery cases shipping today only."""
    df = filter_local_delivery(df)
    if df.empty or 'Ship Date' not in df.columns:
        return df
    today = date.today()
    df = df.copy()
    def match_today(ship_date):
        if pd.isna(ship_date):
            return False
        if hasattr(ship_date, 'date'):
            ship_date = ship_date.date()
        return ship_date == today
    return df[df['Ship Date'].apply(match_today)]


def filter_local_delivery_by_date(df: pd.DataFrame, target_date: date = None) -> pd.DataFrame:
    """Filter to local delivery cases shipping on a specific date."""
    df = filter_local_delivery(df)
    if df.empty or 'Ship Date' not in df.columns:
        return df
    if target_date is None:
        target_date = date.today()
    df = df.copy()
    def match_date(ship_date):
        if pd.isna(ship_date):
            return False
        if hasattr(ship_date, 'date'):
            ship_date = ship_date.date()
        return ship_date == target_date
    return df[df['Ship Date'].apply(match_date)]


def filter_overdue_no_scan(df: pd.DataFrame) -> pd.DataFrame:
    """Filter overdue cases with no scan in last 4 hours.

    Excludes QC location and airway planning locations.
    """
    if df.empty:
        return df

    result = df.copy()
    if 'IsOverdue' not in result.columns:
        result = add_filter_columns(result)

    # Only overdue
    result = result[result['IsOverdue']]

    # Exclude QC
    result = result[result['Last Location'] != 'QC']

    # Exclude airway planning locations
    result = result[~result['Last Location'].isin(MARPE_EXCLUDED_LOCATIONS)]

    # No scan in last 4 hours
    now = datetime.now()
    four_hours_ago = now - timedelta(hours=4)

    def no_recent_scan(scan_time):
        if pd.isna(scan_time):
            return True
        try:
            scan_dt = pd.to_datetime(scan_time)
            return scan_dt < four_hours_ago
        except Exception:
            return True

    result = result[result['Last Scan Time'].apply(no_recent_scan)]

    return result


def build_workload_chart_data(df: pd.DataFrame) -> dict:
    """Build data structure for the workload stacked bar chart."""
    if df.empty:
        return {'labels': [], 'invoiced': [], 'in_production': []}

    # Group by ShipDate and TypeCount (Status)
    labels = sorted(df['ShipDate'].unique())
    invoiced = []
    in_production = []

    for d in labels:
        day_data = df[df['ShipDate'] == d]
        inv = day_data[day_data['TypeCount'] == 'Invoiced']['Count'].sum()
        prod = day_data[day_data['TypeCount'] == 'In Production']['Count'].sum()
        invoiced.append(int(inv))
        in_production.append(int(prod))

    # Format labels
    formatted_labels = []
    for d in labels:
        if hasattr(d, 'strftime'):
            formatted_labels.append(d.strftime('%a %b %d'))
        else:
            formatted_labels.append(str(d))

    return {
        'labels': formatted_labels,
        'invoiced': invoiced,
        'in_production': in_production,
    }


def build_workload_pace_data(df: pd.DataFrame) -> list[dict]:
    """Build per-day pace data: invoiced as percentage of total."""
    if df.empty:
        return []

    labels = sorted(df['ShipDate'].unique())
    pace = []

    for d in labels:
        day_data = df[df['ShipDate'] == d]
        inv = int(day_data[day_data['TypeCount'] == 'Invoiced']['Count'].sum())
        prod = int(day_data[day_data['TypeCount'] == 'In Production']['Count'].sum())
        total = inv + prod
        pct = round((inv / total * 100), 1) if total > 0 else 0

        if hasattr(d, 'strftime'):
            label = d.strftime('%a %b %d')
        else:
            label = str(d)

        pace.append({
            'label': label,
            'invoiced': inv,
            'in_production': prod,
            'total': total,
            'pct': pct,
            'status': 'ahead' if inv >= prod else 'behind',
        })

    return pace[:6]


def build_workload_pivot_table(df: pd.DataFrame) -> dict:
    """Build pivot table data for workload category breakdown."""
    if df.empty:
        return {'dates': [], 'categories': [], 'data': {}, 'totals': {}}

    dates = sorted(df['ShipDate'].unique())
    categories_order = [
        'Metal', 'Clear', 'Wire Bending', 'MARPE',
        'E2 Expanders', 'Hybrid', 'Other', 'Lab to Lab', 'Airway',
    ]

    # Get actual categories present
    actual_cats = df['Category'].unique().tolist() if 'Category' in df.columns else []
    categories = [c for c in categories_order if c in actual_cats]
    for c in actual_cats:
        if c not in categories:
            categories.append(c)

    data = {}
    totals = {}
    for cat in categories:
        row = []
        for d in dates:
            val = df[(df['Category'] == cat) & (df['ShipDate'] == d)]['CaseCount'].sum()
            row.append(int(val))
        data[cat] = row

    for i, d in enumerate(dates):
        totals[i] = sum(data[cat][i] for cat in categories)

    formatted_dates = []
    for d in dates:
        if hasattr(d, 'strftime'):
            formatted_dates.append(d.strftime('%b %d'))
        else:
            formatted_dates.append(str(d))

    return {
        'dates': formatted_dates,
        'categories': categories,
        'data': data,
        'totals': totals,
    }


def build_sales_history(df: pd.DataFrame, num_days: int = 5) -> list[dict]:
    """Build last N business days of sales from daily_sales data.

    Uses Type 'I' (invoice) rows for accurate revenue and case counts.
    Sums NumberOfInvoices and SubTotal across all LabNames per day.
    """
    if df.empty:
        return []

    # Use 'I' (invoice) rows for revenue and case counts
    df = df[df['Type'] == 'I']
    if df.empty:
        return []

    holidays = get_all_company_holidays()

    # Aggregate per date across all LabNames
    daily = df.groupby('SalesDate').agg(
        invoice_count=('NumberOfInvoices', 'sum'),
        subtotal=('SubTotal', 'sum'),
    ).reset_index()

    # Filter to business days only
    daily = daily[
        daily['SalesDate'].apply(lambda d: d.weekday() < 5 and d not in holidays)
    ].sort_values('SalesDate', ascending=False)

    result = []
    for _, row in daily.head(num_days).iterrows():
        d = row['SalesDate']
        label = d.strftime('%a %b %d') if hasattr(d, 'strftime') else str(d)
        result.append({
            'label': label,
            'count': int(row['invoice_count']),
            'subtotal': round(float(row['subtotal']), 2),
        })

    return list(reversed(result))  # chronological order
