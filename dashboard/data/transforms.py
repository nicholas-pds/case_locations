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


def adjust_rush_ship_dates(df: pd.DataFrame, ship_col: str = 'ShipDate') -> pd.DataFrame:
    """Adjust ShipDate for rush pans to the previous business day (holiday-aware)."""
    if df.empty or ship_col not in df.columns:
        return df
    df = df.copy()
    pan_col = 'PanNumber' if 'PanNumber' in df.columns else 'Pan Number'
    if pan_col not in df.columns:
        return df

    holidays = get_all_company_holidays()
    mask = df[pan_col].apply(is_rush)

    for idx in df[mask].index:
        ship = df.at[idx, ship_col]
        if pd.notna(ship):
            if hasattr(ship, 'date'):
                ship = ship.date()
            df.at[idx, ship_col] = previous_business_day(ship, holidays)

    return df


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
                 category: str = None, ship_date: str = None) -> pd.DataFrame:
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
        if location == 'No Location':
            result = result[
                result['Last Location'].isna() |
                (result['Last Location'].astype(str).str.strip() == '')
            ]
        else:
            result = result[result['Last Location'] == location]

    if category:
        result = result[result['Category'] == category]

    if ship_date:
        def _matches_ship_date(val):
            if pd.isna(val) or val is None:
                return False
            try:
                if isinstance(val, str):
                    val = pd.to_datetime(val)
                if hasattr(val, 'strftime'):
                    return val.strftime(_DATE_FMT) == ship_date
            except Exception:
                pass
            return False
        result = result[result['Ship Date'].apply(_matches_ship_date)]

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

    # Include locations in the display order first
    for loc in LOCATION_DISPLAY_ORDER:
        if loc in location_data:
            result.append(location_data[loc])

    # Append any extra locations not in the display order
    for loc_name, loc_info in location_data.items():
        if loc_name not in LOCATION_DISPLAY_ORDER:
            result.append(loc_info)

    # Add a bucket for cases with null/blank location
    null_mask = df['Last Location'].isna() | (df['Last Location'].astype(str).str.strip() == '')
    null_cases = df[null_mask]
    if not null_cases.empty:
        cats = null_cases['Category'].value_counts().to_dict()
        result.append({
            'name': 'No Location',
            'total': len(null_cases),
            'categories': cats,
        })

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


def _pace_status(pct: float) -> str:
    """Return 4-tier color status based on percentage."""
    if pct <= 25:
        return 'red'
    elif pct <= 50:
        return 'orange'
    elif pct <= 75:
        return 'yellow'
    return 'green'


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
            'status': _pace_status(pct),
        })

    return pace[:6]


def build_workload_pivot_table(df: pd.DataFrame) -> dict:
    """Build pivot table data for workload category breakdown."""
    if df.empty:
        return {'dates': [], 'categories': [], 'data': {}, 'totals': {}}

    # Pivot table shows only In Production cases
    if 'Status' in df.columns:
        df = df[df['Status'] == 'In Production']
    if df.empty:
        return {'dates': [], 'categories': [], 'data': {}, 'totals': {}}

    dates = sorted(df['ShipDate'].unique())
    categories_order = [
        'Hybrid', 'E2 Expanders', 'Lab to Lab', 'MARPE',
        'Metal', 'Clear', 'Wire Bending', 'Other',
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


def build_category_pace_data(df: pd.DataFrame) -> list[dict]:
    """Build per-category pace rows from the workload_pivot data.

    Expects a DataFrame with columns: Category, Status, ShipDate, CaseCount.
    Returns a list of dicts, one per category, each containing 'category' and
    'days' (a list of pace-tile dicts identical to build_workload_pace_data).
    """
    if df.empty:
        return []

    categories_order = [
        'Hybrid', 'E2 Expanders', 'Lab to Lab', 'MARPE',
        'Metal', 'Clear', 'Wire Bending', 'Other',
    ]

    dates = sorted(df['ShipDate'].unique())
    actual_cats = df['Category'].unique().tolist() if 'Category' in df.columns else []
    categories = [c for c in categories_order if c in actual_cats]
    for c in actual_cats:
        if c not in categories:
            categories.append(c)

    result = []
    for cat in categories:
        cat_df = df[df['Category'] == cat]
        days = []
        for d in dates:
            day_data = cat_df[cat_df['ShipDate'] == d]
            inv = int(day_data[day_data['Status'] == 'Invoiced']['CaseCount'].sum())
            prod = int(day_data[day_data['Status'] == 'In Production']['CaseCount'].sum())
            total = inv + prod
            pct = round((inv / total * 100), 1) if total > 0 else 0

            if hasattr(d, 'strftime'):
                label = d.strftime('%a %b %d')
            else:
                label = str(d)

            days.append({
                'label': label,
                'invoiced': inv,
                'in_production': prod,
                'total': total,
                'pct': pct,
                'status': _pace_status(pct),
            })

        result.append({
            'category': cat,
            'days': days[:6],
        })

    return result


_MONTH_ABBR = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
               'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']


def _goals_color(pct: float | None) -> str:
    """Return hex color for a % of goal value."""
    if pct is None:
        return '#95A5A6'  # gray — no data
    if pct >= 110:
        return '#1B5E20'  # dark green
    if pct >= 100:
        return '#27AE60'  # green
    if pct >= 90:
        return '#F39C12'  # amber
    return '#C0392B'  # red


def build_monthly_sales_chart(df: pd.DataFrame, num_months: int = 18) -> dict:
    """Build last N months of invoice revenue for Chart A.

    Returns {labels, data, trend, is_current} where:
    - labels: list of "Mon 'YY" strings
    - data: list of SubTotal floats
    - trend: 3-month rolling average (None where not enough data)
    - is_current: list of bools (True for the partial current month)
    """
    if df is None or df.empty:
        return {'labels': [], 'data': [], 'trend': [], 'is_current': []}

    df = df.copy().sort_values(['SalesYear', 'SalesMonth']).reset_index(drop=True)

    today = date.today()
    current_year = today.year
    current_month = today.month

    # Take last num_months+1 rows (the extra row feeds the rolling avg for month 1)
    window_df = df.tail(num_months + 1).reset_index(drop=True)

    # Compute 3-month rolling average across the full window
    subtotals = window_df['SubTotal'].tolist()
    rolling = []
    for i in range(len(subtotals)):
        if i < 2:
            rolling.append(None)
        else:
            rolling.append(round((subtotals[i] + subtotals[i-1] + subtotals[i-2]) / 3, 2))

    # Trim to last num_months rows
    display_df = window_df.tail(num_months).reset_index(drop=True)
    display_rolling = rolling[-num_months:]

    labels = []
    data = []
    trend = []
    is_current = []

    for i, row in display_df.iterrows():
        yr = int(row['SalesYear'])
        mo = int(row['SalesMonth'])
        labels.append(f"{_MONTH_ABBR[mo - 1]} '{str(yr)[2:]}")
        data.append(round(float(row['SubTotal']), 2))
        trend.append(display_rolling[i])
        is_current.append(yr == current_year and mo == current_month)

    return {'labels': labels, 'data': data, 'trend': trend, 'is_current': is_current}


def build_daily_sales_chart(df: pd.DataFrame, num_days: int = 30) -> dict:
    """Build last N calendar days of invoice revenue for Chart B.

    Returns {labels, data, trend, is_today} where:
    - labels: list of "DD Mon YY" strings
    - data: list of SubTotal floats
    - trend: 7-day rolling average (None where not enough data)
    - is_today: list of bools (True for today's partial bar)
    """
    if df is None or df.empty:
        return {'labels': [], 'data': [], 'trend': [], 'is_today': []}

    df = df.copy()
    df = df[df['Type'] == 'I']
    if df.empty:
        return {'labels': [], 'data': [], 'trend': [], 'is_today': []}

    daily = df.groupby('SalesDate').agg(subtotal=('SubTotal', 'sum')).reset_index()
    daily = daily.sort_values('SalesDate').reset_index(drop=True)

    today = date.today()
    cutoff = today - timedelta(days=num_days - 1)
    daily = daily[daily['SalesDate'] >= cutoff].reset_index(drop=True)

    # Fill missing calendar days with 0
    if not daily.empty:
        all_dates = pd.date_range(start=cutoff, end=today, freq='D').date
        date_index = {r['SalesDate']: r['subtotal'] for _, r in daily.iterrows()}
        full_dates = []
        full_vals = []
        for d in all_dates:
            full_dates.append(d)
            full_vals.append(float(date_index.get(d, 0)))
    else:
        full_dates = []
        full_vals = []

    # 7-day rolling average
    rolling = []
    for i in range(len(full_vals)):
        if i < 6:
            rolling.append(None)
        else:
            rolling.append(round(sum(full_vals[i-6:i+1]) / 7, 2))

    labels = []
    is_today = []
    for d in full_dates:
        dt = d if isinstance(d, date) else d.date()
        labels.append(dt.strftime('%d %b %y'))
        is_today.append(dt == today)

    return {
        'labels': labels,
        'data': [round(v, 2) for v in full_vals],
        'trend': rolling,
        'is_today': is_today,
    }


def build_monthly_goals_chart(df_sales: pd.DataFrame, df_goals: pd.DataFrame) -> dict:
    """Build current-year monthly revenue vs goals for Chart C.

    Returns {labels, goals, actuals, colors, pct_of_goals, year}.
    """
    today = date.today()
    current_year = today.year
    current_month = today.month

    labels = [_MONTH_ABBR[m - 1] for m in range(1, 13)]

    # Build goals lookup for current year
    if df_goals is not None and not df_goals.empty:
        yr_goals = df_goals[df_goals['Year'] == current_year]
        goal_lookup = {int(r['Month']): float(r['RevenueGoal']) for _, r in yr_goals.iterrows()}
    else:
        goal_lookup = {}

    # Build actuals lookup for current year
    if df_sales is not None and not df_sales.empty:
        yr_sales = df_sales[df_sales['SalesYear'] == current_year]
        actual_lookup = {int(r['SalesMonth']): float(r['SubTotal']) for _, r in yr_sales.iterrows()}
    else:
        actual_lookup = {}

    goals = []
    actuals = []
    colors = []
    pct_of_goals = []

    for mo in range(1, 13):
        goal = goal_lookup.get(mo)
        actual = actual_lookup.get(mo) if mo <= current_month else None

        goals.append(goal if goal is not None else 0)
        actuals.append(actual if actual is not None else None)

        if actual is not None and goal and goal > 0:
            pct = actual / goal * 100
            pct_of_goals.append(round(pct, 1))
            colors.append(_goals_color(pct))
        elif mo > current_month:
            pct_of_goals.append(None)
            colors.append('#95A5A6')
        else:
            pct_of_goals.append(None)
            colors.append('#95A5A6')

    return {
        'labels': labels,
        'goals': goals,
        'actuals': actuals,
        'colors': colors,
        'pct_of_goals': pct_of_goals,
        'year': current_year,
    }


def build_annual_goals_chart(df_sales: pd.DataFrame, df_goals: pd.DataFrame,
                              num_years: int = 5) -> dict:
    """Build annual revenue vs goals for Chart D (last N years).

    Returns {labels, goals, actuals, colors, pct_of_goals}.
    """
    today = date.today()
    current_year = today.year
    start_year = current_year - num_years + 1

    years = list(range(start_year, current_year + 1))
    labels = [str(y) if y != current_year else f"{y} (YTD)" for y in years]

    # Aggregate goals by year
    if df_goals is not None and not df_goals.empty:
        goals_by_year = df_goals.groupby('Year')['RevenueGoal'].sum().to_dict()
    else:
        goals_by_year = {}

    # Aggregate sales by year
    if df_sales is not None and not df_sales.empty:
        sales_by_year = df_sales.groupby('SalesYear')['SubTotal'].sum().to_dict()
    else:
        sales_by_year = {}

    goals = []
    actuals = []
    colors = []
    pct_of_goals = []

    for yr in years:
        goal = float(goals_by_year.get(yr, 0))
        actual = float(sales_by_year.get(yr, 0))

        goals.append(goal)
        actuals.append(actual)

        if goal > 0:
            pct = actual / goal * 100
            pct_of_goals.append(round(pct, 1))
            colors.append(_goals_color(pct))
        else:
            pct_of_goals.append(None)
            colors.append('#95A5A6')

    return {
        'labels': labels,
        'goals': goals,
        'actuals': actuals,
        'colors': colors,
        'pct_of_goals': pct_of_goals,
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
