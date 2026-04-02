"""Wraps src/db_handler for each SQL query with appropriate transforms."""
import sys
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

# Add project root to path so we can import src modules
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.db_handler import execute_sql_to_dataframe
from src.holidays import previous_business_day
from src.date_parser import process_dataframe, sort_by_follow_up_date
from dashboard.config import (
    SQL_DIR, DAYS_LOOKBACK, WORKLOAD_DAYS_RANGE,
    MARPE_EXCLUDED_LOCATIONS, LOCATION_ALIASES,
)
from dashboard.data.transforms import adjust_rush_ship_dates


def fetch_case_locations() -> pd.DataFrame:
    """Execute case_locs_1.sql and apply transforms (Query 1).
    NOTE: This is the ONLY fetch that does NOT adjust rush ShipDates.
    The case locations table displays actual unadjusted dates."""
    df = execute_sql_to_dataframe(str(SQL_DIR / "case_locs_1.sql"))
    if df.empty:
        return df

    # Date range filter
    cutoff_date = datetime.now() - timedelta(days=DAYS_LOOKBACK)
    df['Ship Date'] = pd.to_datetime(df['Ship Date'])
    df = df[df['Ship Date'] > cutoff_date]
    df['Ship Date'] = df['Ship Date'].dt.date

    # Normalize categories
    if 'Category' in df.columns:
        # Blank / empty → Other
        df['Category'] = df['Category'].fillna('').str.strip()
        df.loc[df['Category'] == '', 'Category'] = 'Other'

        # Normalize E*Expander* variants to display name with superscript
        expander_mask = df['Category'].str.contains('Expander', case=False, na=False)
        df.loc[expander_mask, 'Category'] = 'E² Expanders'

        df['Category'] = df['Category'].replace({'Airway': 'MARPE', 'Lab to lab': 'Lab to Lab'})

        # Filter out MARPE rows in planning stages
        mask = ~(
            (df['Category'] == 'MARPE') &
            (df['Last Location'].isin(MARPE_EXCLUDED_LOCATIONS))
        )
        df = df[mask]

    # Apply location aliases (e.g. Marpe sub-locations -> Marpe)
    if 'Last Location' in df.columns:
        df['Last Location'] = df['Last Location'].replace(LOCATION_ALIASES)

    return df.reset_index(drop=True)


def fetch_workload_status() -> pd.DataFrame:
    """Execute cases_Prod_and_Invoiced.sql (Query 2).
    Returns per-row data, adjusts rush ShipDates, then aggregates."""
    df = execute_sql_to_dataframe(str(SQL_DIR / "cases_Prod_and_Invoiced.sql"))
    if df.empty:
        return df

    df['ShipDate'] = pd.to_datetime(df['ShipDate']).dt.date

    # Adjust rush pan ShipDates to previous business day (holiday-aware)
    df = adjust_rush_ship_dates(df, 'ShipDate')

    # Filter date range
    prev_biz_day = previous_business_day()
    start_date = prev_biz_day
    end_date = prev_biz_day + timedelta(days=WORKLOAD_DAYS_RANGE)
    df = df[(df['ShipDate'] >= start_date) & (df['ShipDate'] <= end_date)]

    # Aggregate: group by TypeCount + ShipDate, count rows
    df = df.groupby(['TypeCount', 'ShipDate']).size().reset_index(name='Count')

    return df.reset_index(drop=True)


def fetch_workload_pivot() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Execute workload_pivot.sql for category breakdown.
    Returns (aggregated_df, detail_df) — per-row data with rush adjustment,
    then aggregated counts for tiles."""
    df = execute_sql_to_dataframe(str(SQL_DIR / "workload_pivot.sql"))
    if df.empty:
        return df, df

    df['ShipDate'] = pd.to_datetime(df['ShipDate']).dt.date
    if 'DueDate' in df.columns:
        df['DueDate'] = pd.to_datetime(df['DueDate'], errors='coerce').dt.date

    # Save original ShipDate before rush adjustment (for display in pace modal)
    df['OrigShipDate'] = df['ShipDate']

    # Adjust rush pan ShipDates to previous business day (holiday-aware)
    df = adjust_rush_ship_dates(df, 'ShipDate')

    # Filter date range
    prev_biz_day = previous_business_day()
    start_date = prev_biz_day
    end_date = prev_biz_day + timedelta(days=WORKLOAD_DAYS_RANGE)
    df = df[(df['ShipDate'] >= start_date) & (df['ShipDate'] <= end_date)]

    # Normalize categories
    if 'Category' in df.columns:
        df['Category'] = df['Category'].fillna('').str.strip()
        df.loc[df['Category'] == '', 'Category'] = 'Other'

        expander_mask = df['Category'].str.contains('Expander', case=False, na=False)
        df.loc[expander_mask, 'Category'] = 'E² Expanders'

    # Keep per-row detail for pace modal
    detail_df = df.copy()

    # Aggregate: group by Category + Status + ShipDate, count cases
    agg_df = df.groupby(['Category', 'Status', 'ShipDate']).size().reset_index(name='CaseCount')

    return agg_df.reset_index(drop=True), detail_df.reset_index(drop=True)


def fetch_airway_workflow() -> pd.DataFrame:
    """Execute case_locs_airway_1.sql (Query 3)."""
    df = execute_sql_to_dataframe(str(SQL_DIR / "case_locs_airway_1.sql"))
    if df.empty:
        return df

    df['ShipDate'] = pd.to_datetime(df['ShipDate'], errors='coerce').dt.date

    # Adjust rush pan ShipDates to previous business day (holiday-aware)
    df = adjust_rush_ship_dates(df, 'ShipDate')

    return df.reset_index(drop=True)


def fetch_airway_hold_status() -> pd.DataFrame:
    """Execute airway_hold_status_1.sql with follow-up date parsing (Query 4)."""
    df = execute_sql_to_dataframe(str(SQL_DIR / "airway_hold_status_1.sql"))
    if df.empty:
        return df

    if 'TYPE' in df.columns:
        df = df.rename(columns={'TYPE': 'FollowUpType'})

    # Adjust rush pan ShipDates if columns exist
    if 'ShipDate' in df.columns:
        df['ShipDate'] = pd.to_datetime(df['ShipDate'], errors='coerce').dt.date
        df = adjust_rush_ship_dates(df, 'ShipDate')

    df = process_dataframe(df)
    df = sort_by_follow_up_date(df)

    return df.reset_index(drop=True)


def fetch_submitted_cases() -> pd.DataFrame:
    """Fetch cases with 'Submitted' status."""
    df = execute_sql_to_dataframe(str(SQL_DIR / "cases_submitted.sql"))
    if df.empty:
        return df
    if 'Ship Date' in df.columns:
        df['Ship Date'] = pd.to_datetime(df['Ship Date'], errors='coerce')
        df['Ship Date'] = df['Ship Date'].dt.date
        # Adjust rush pan ShipDates to previous business day (holiday-aware)
        df = adjust_rush_ship_dates(df, 'Ship Date')
    return df.reset_index(drop=True)


def fetch_daily_sales() -> pd.DataFrame:
    """Fetch daily sales data (invoice + production) for the past ~2 weeks."""
    df = execute_sql_to_dataframe(str(SQL_DIR / "daily_sales.sql"))
    if df.empty:
        return df
    df['SalesDate'] = pd.to_datetime(df['SalesDate'])
    df['SalesDate'] = df['SalesDate'].dt.date
    return df.reset_index(drop=True)


def fetch_monthly_sales() -> pd.DataFrame:
    """Fetch monthly invoice revenue aggregation for the last 19 months."""
    df = execute_sql_to_dataframe(str(SQL_DIR / "monthly_sales.sql"))
    if df.empty:
        return df
    df['SalesYear'] = df['SalesYear'].astype(int)
    df['SalesMonth'] = df['SalesMonth'].astype(int)
    df['SubTotal'] = pd.to_numeric(df['SubTotal'], errors='coerce').fillna(0)
    return df.reset_index(drop=True)


def fetch_customers() -> pd.DataFrame:
    """Fetch all active customers joined with sales summary data."""
    df = execute_sql_to_dataframe(str(SQL_DIR / "AM_customers_all.sql"))
    if df.empty:
        return df
    for col in ['DateOfFirstCase', 'DateOfLastCase']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
    for col in ['MTDSales', 'LMSales', 'YTDSales', 'LySales', 'LTDSales']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    return df.reset_index(drop=True)
