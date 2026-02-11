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
    MARPE_EXCLUDED_LOCATIONS,
)


def fetch_case_locations() -> pd.DataFrame:
    """Execute case_locs_1.sql and apply transforms (Query 1)."""
    df = execute_sql_to_dataframe(str(SQL_DIR / "case_locs_1.sql"))
    if df.empty:
        return df

    # Date range filter
    cutoff_date = datetime.now() - timedelta(days=DAYS_LOOKBACK)
    df['Ship Date'] = pd.to_datetime(df['Ship Date'])
    df = df[df['Ship Date'] > cutoff_date]
    df['Ship Date'] = df['Ship Date'].dt.date

    # Replace Airway -> MARPE in Category
    if 'Category' in df.columns:
        df['Category'] = df['Category'].replace('Airway', 'MARPE')
        df['Category'] = df['Category'].fillna('Other')

        # Filter out MARPE rows in planning stages
        mask = ~(
            (df['Category'] == 'MARPE') &
            (df['Last Location'].isin(MARPE_EXCLUDED_LOCATIONS))
        )
        df = df[mask]

    return df.reset_index(drop=True)


def fetch_workload_status() -> pd.DataFrame:
    """Execute cases_Prod_and_Invoiced.sql (Query 2)."""
    df = execute_sql_to_dataframe(str(SQL_DIR / "cases_Prod_and_Invoiced.sql"))
    if df.empty:
        return df

    prev_biz_day = previous_business_day()
    start_date = prev_biz_day
    end_date = prev_biz_day + timedelta(days=WORKLOAD_DAYS_RANGE)

    df['ShipDate'] = pd.to_datetime(df['ShipDate'])
    df = df[
        (df['ShipDate'].dt.date >= start_date) &
        (df['ShipDate'].dt.date <= end_date)
    ]
    df['ShipDate'] = df['ShipDate'].dt.date

    return df.reset_index(drop=True)


def fetch_workload_pivot() -> pd.DataFrame:
    """Execute workload_pivot.sql for category breakdown."""
    df = execute_sql_to_dataframe(str(SQL_DIR / "workload_pivot.sql"))
    if df.empty:
        return df

    prev_biz_day = previous_business_day()
    start_date = prev_biz_day
    end_date = prev_biz_day + timedelta(days=WORKLOAD_DAYS_RANGE)

    df['ShipDate'] = pd.to_datetime(df['ShipDate'])
    df = df[
        (df['ShipDate'].dt.date >= start_date) &
        (df['ShipDate'].dt.date <= end_date)
    ]
    df['ShipDate'] = df['ShipDate'].dt.date

    return df.reset_index(drop=True)


def fetch_airway_workflow() -> pd.DataFrame:
    """Execute case_locs_airway_1.sql (Query 3)."""
    return execute_sql_to_dataframe(str(SQL_DIR / "case_locs_airway_1.sql"))


def fetch_airway_hold_status() -> pd.DataFrame:
    """Execute airway_hold_status_1.sql with follow-up date parsing (Query 4)."""
    df = execute_sql_to_dataframe(str(SQL_DIR / "airway_hold_status_1.sql"))
    if df.empty:
        return df

    if 'TYPE' in df.columns:
        df = df.rename(columns={'TYPE': 'FollowUpType'})

    df = process_dataframe(df)
    df = sort_by_follow_up_date(df)

    return df.reset_index(drop=True)
