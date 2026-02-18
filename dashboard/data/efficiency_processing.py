"""
Efficiency report processing pipeline.
Implements Stages 1-4 from the instructions document, plus midday snapshot processing.
"""
import re
import logging
import os
import io
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Literal

import pandas as pd
import pyodbc
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("dashboard.efficiency_processing")

# Path to employee lookup CSV
_LOOKUP_PATH = Path(__file__).parent.parent.parent / "User_Inputs" / "employee_lkups.csv"
_SQL_DIR = Path(__file__).parent.parent.parent / "sql_query"

# ─────────────────────────────────────────────
# Employee Lookup
# ─────────────────────────────────────────────

def load_employee_lookup() -> pd.DataFrame:
    """Load the employee lookup table from CSV."""
    return pd.read_csv(_LOOKUP_PATH, dtype={"Employee ID": int, "Training Plan": int})


# ─────────────────────────────────────────────
# SQL Connection
# ─────────────────────────────────────────────

def _get_conn():
    driver = "{ODBC Driver 17 for SQL Server}"
    conn_str = (
        f"DRIVER={driver};"
        f"SERVER={os.getenv('SQL_SERVER')};"
        f"DATABASE={os.getenv('SQL_DATABASE')};"
        f"UID={os.getenv('SQL_USERNAME')};"
        f"PWD={os.getenv('SQL_PASSWORD')}"
    )
    return pyodbc.connect(conn_str)


def _fetch_tasks(start_date: str, end_date: str) -> pd.DataFrame:
    """Fetch daily task data for a date range. Dates as 'YYYY-MM-DD'."""
    sql = (_SQL_DIR / "efficiency_tasks.sql").read_text()
    start_param = f"{start_date} 00:00:00"
    # end_date from filename + 1 day (exclusive upper bound)
    end_dt = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
    end_param = end_dt.strftime("%Y-%m-%d 00:00:00")
    conn = _get_conn()
    try:
        df = pd.read_sql(sql, conn, params=[start_param, end_param])
    finally:
        conn.close()
    return df


def _fetch_midday_raw() -> pd.DataFrame:
    """Fetch midday snapshot data (5-day lookback, no date params)."""
    sql = (_SQL_DIR / "efficiency_midday.sql").read_text()
    conn = _get_conn()
    try:
        df = pd.read_sql(sql, conn)
    finally:
        conn.close()
    return df


# ─────────────────────────────────────────────
# Stage 1: Task Processing
# ─────────────────────────────────────────────

def stage1_task_processing(raw_df: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
    """Process raw SQL task data into per-employee aggregates."""
    if raw_df.empty:
        return pd.DataFrame(columns=["EmployeeID", "Cases_Worked_On", "Tasks_Completed", "Tasks_Duration_Hours"])

    # 1a. Rename columns
    df = raw_df.rename(columns={
        "CompletedBY": "EmployeeID",
        "CaseNumber": "CaseNumber",
        "completeDate": "CompleteDate",
        "task": "Task",
        "rejected": "Rejected",
        "Quantity": "Quantity",
        "CaseProductID": "CaseProductID",
        "Duration": "Duration",
    })

    # 1b. Duration per row
    df["Duration"] = pd.to_numeric(df["Duration"], errors="coerce").fillna(0)

    # 1c. Convert Rejected to int
    if df["Rejected"].dtype == object:
        df["Rejected"] = df["Rejected"].map({"Yes": 1, "No": 0}).fillna(0).astype(int)
    else:
        df["Rejected"] = df["Rejected"].fillna(0).astype(int)

    # 1d. Filter by date range (safety check)
    df["CompleteDate"] = pd.to_datetime(df["CompleteDate"], errors="coerce")
    start_dt = pd.to_datetime(start_date)
    end_dt = pd.to_datetime(end_date) + timedelta(days=1)
    df = df[(df["CompleteDate"] >= start_dt) & (df["CompleteDate"] < end_dt)]

    # 1e. Anti-join rejected tasks
    rejected = df[df["Rejected"] == 1][["CaseNumber", "CompleteDate", "Task", "EmployeeID"]].drop_duplicates()
    if not rejected.empty:
        merged = df.merge(rejected, on=["CaseNumber", "CompleteDate", "Task", "EmployeeID"], how="left", indicator=True)
        df = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])

    # 1f. Aggregate by EmployeeID
    agg = df.groupby("EmployeeID").agg(
        Cases_Worked_On=("CaseNumber", "nunique"),
        Tasks_Completed=("CaseNumber", "count"),
        Tasks_Duration_Hours=("Duration", lambda x: round(x.sum(), 2)),
    ).reset_index()

    # 1g. Sort and convert EmployeeID to string
    agg = agg.sort_values("EmployeeID").reset_index(drop=True)
    agg["EmployeeID"] = agg["EmployeeID"].astype(str)
    return agg


# ─────────────────────────────────────────────
# Stage 2: Gusto Hours Processing
# ─────────────────────────────────────────────

def parse_gusto_filename(filename: str):
    """Extract start_date and end_date strings from Gusto CSV filename."""
    m = re.search(r"(\d{4}-\d{2}-\d{2})-to-(\d{4}-\d{2}-\d{2})", filename)
    if not m:
        raise ValueError(f"Cannot extract date range from filename: {filename}")
    return m.group(1), m.group(2)


def stage2_gusto_processing(gusto_bytes: bytes, filename: str) -> pd.DataFrame:
    """Parse Gusto CSV and join with employee lookup."""
    start_date, _ = parse_gusto_filename(filename)

    # Parse CSV: skip first 8 rows, row 9 is header
    raw = pd.read_csv(
        io.BytesIO(gusto_bytes),
        skiprows=8,
        dtype=str,
    )

    # Truncate at first completely blank row
    blank_mask = raw.isnull().all(axis=1)
    if blank_mask.any():
        first_blank = blank_mask.idxmax()
        raw = raw.iloc[:first_blank]

    # 2a. Calculate Work Hours
    raw["Total hours"] = pd.to_numeric(raw["Total hours"], errors="coerce")
    raw["Rest break"] = pd.to_numeric(raw["Rest break"], errors="coerce")
    raw["Work Hours"] = (raw["Total hours"] - raw["Rest break"].fillna(0)).fillna(0)

    # 2b. Select columns
    gusto = raw[["Name", "Work Hours"]].copy()

    # 2c. Join with employee lookup
    lookup = load_employee_lookup()
    gusto = gusto.merge(
        lookup[["Gusto Name", "Employee ID", "MT Name", "Team", "Training Plan"]],
        left_on="Name",
        right_on="Gusto Name",
        how="left",
    )

    # 2d. Clean up
    gusto["Employee ID"] = gusto["Employee ID"].fillna(0).astype(int)
    gusto["Training Plan"] = gusto["Training Plan"].fillna(0).astype(int)

    # 2e. Rename
    gusto = gusto.rename(columns={"Employee ID": "EmployeeID"})

    # 2f. Set date
    gusto["Date"] = start_date

    # 2g. Final column order
    gusto = gusto[["Date", "EmployeeID", "Gusto Name", "MT Name", "Team", "Training Plan", "Work Hours"]]
    return gusto


# ─────────────────────────────────────────────
# Stage 3: Combine & Calculate Efficiency
# ─────────────────────────────────────────────

def stage3_combine(task_df: pd.DataFrame, gusto_df: pd.DataFrame) -> pd.DataFrame:
    """Inner join task and Gusto data, calculate efficiency."""
    # 3a. Ensure EmployeeID is string in both
    task_df = task_df.copy()
    gusto_df = gusto_df.copy()
    task_df["EmployeeID"] = task_df["EmployeeID"].astype(str)
    gusto_df["EmployeeID"] = gusto_df["EmployeeID"].astype(str)

    # 3b. Inner join
    merged = gusto_df.merge(task_df, on="EmployeeID", how="inner")

    # 3c. Reorder columns
    cols = ["Date", "EmployeeID", "Gusto Name", "MT Name", "Team", "Training Plan",
            "Work Hours", "Cases_Worked_On", "Tasks_Completed", "Tasks_Duration_Hours"]
    merged = merged[cols]

    # 3d. Calculate efficiency
    merged["Efficiency"] = (merged["Tasks_Duration_Hours"] / merged["Work Hours"].replace(0, float("nan"))) * 100
    merged["Efficiency"] = merged["Efficiency"].fillna(0).round(2)
    merged["Work Hours"] = merged["Work Hours"].round(2)

    return merged


# ─────────────────────────────────────────────
# Stage 4: Multi-Period Aggregated Efficiency
# ─────────────────────────────────────────────

def _get_business_days_ago(n: int, reference: date) -> date:
    """Return the date N business days before reference, skipping weekends and holidays."""
    from src.holidays import get_all_company_holidays
    holidays = get_all_company_holidays()
    candidate = reference - timedelta(days=1)
    count = 0
    while count < n:
        if candidate.weekday() < 5 and candidate not in holidays:
            count += 1
        if count < n:
            candidate -= timedelta(days=1)
    return candidate


def _eff_for_period(df: pd.DataFrame, name: str, start: date, end: date) -> float:
    """Mean non-zero efficiency for an employee in a date range."""
    mask = (df["MT Name"] == name) & (df["Date"] >= pd.Timestamp(start)) & (df["Date"] <= pd.Timestamp(end))
    rows = df[mask]
    non_zero = rows[rows["Efficiency"] != 0]["Efficiency"]
    if non_zero.empty:
        return float("nan")
    return non_zero.mean()


def stage4_aggregated(all_daily_df: pd.DataFrame, reference: date = None) -> pd.DataFrame:
    """Build multi-period efficiency analysis from all historical daily data."""
    if all_daily_df.empty:
        return pd.DataFrame()

    if reference is None:
        reference = date.today()

    df = all_daily_df.copy()
    df["Date"] = pd.to_datetime(df["Date"])

    # 4b. Unique employees with their team
    employees = df.groupby("MT Name")["Team"].first().reset_index()

    # 4c & 4d. Calculate 15 efficiency metrics per employee
    rows = []
    for _, emp in employees.iterrows():
        name = emp["MT Name"]
        team = emp["Team"]
        row = {"MT Name": name, "Team": team}

        # Single days: 1–5 business days ago
        for n in range(1, 6):
            day = _get_business_days_ago(n, reference)
            row[f"Efficiency_{n}_Day_Ago"] = _eff_for_period(df, name, day, day)

        # Last week average: 7 calendar days back to 1 day back
        week_end = reference - timedelta(days=1)
        week_start = reference - timedelta(days=7)
        row["Efficiency_Last_Week_Average"] = _eff_for_period(df, name, week_start, week_end)

        # Month to date
        mtd_start = date(reference.year, reference.month, 1)
        row["Efficiency_Month_To_Date"] = _eff_for_period(df, name, mtd_start, reference)

        # Previous months: 1–12 months ago
        for m in range(1, 13):
            month_ref = reference.month - m
            year_ref = reference.year + (month_ref - 1) // 12
            month_ref = ((month_ref - 1) % 12) + 1
            month_start = date(year_ref, month_ref, 1)
            # Last day of that month
            if month_ref == 12:
                month_end = date(year_ref + 1, 1, 1) - timedelta(days=1)
            else:
                month_end = date(year_ref, month_ref + 1, 1) - timedelta(days=1)
            label = "Efficiency_Previous_Month" if m == 1 else f"Efficiency_{m}_Months_Ago"
            row[label] = _eff_for_period(df, name, month_start, month_end)

        rows.append(row)

    result = pd.DataFrame(rows)

    # 4e. Add Training Plan from most recent date
    max_date = df["Date"].max()
    latest = df[df["Date"] == max_date][["MT Name", "Training Plan"]].drop_duplicates("MT Name")
    result = result.merge(latest, on="MT Name", how="left")
    result["Training Plan"] = result["Training Plan"].fillna(0).astype(int)

    # Reorder: MT Name, Team, Training Plan, then efficiency cols
    eff_cols = [c for c in result.columns if c.startswith("Efficiency_")]
    result = result[["MT Name", "Team", "Training Plan"] + eff_cols]

    # 4f. Convert percentages to decimals
    for col in eff_cols:
        result[col] = (result[col] * 0.01).round(2)

    # 4g. NaN handling
    for col in eff_cols:
        if result[col].isna().all():
            result[col] = 0
        else:
            result[col] = result[col].where(result[col].notna(), other="x")

    # Drop columns that are entirely 0 (no data at all)
    cols_to_drop = [c for c in eff_cols if (result[c] == 0).all()]
    result = result.drop(columns=cols_to_drop)

    return result.reset_index(drop=True)


# ─────────────────────────────────────────────
# Midday Snapshot Processing
# ─────────────────────────────────────────────

def process_midday_snapshot(window: Literal["noon", "3pm"]) -> pd.DataFrame:
    """
    Fetch midday task data from SQL and filter to today's time window.
    window='noon'  → 3:00 AM – 12:00 PM
    window='3pm'   → 3:00 AM – 3:00 PM
    Returns flat task records grouped by employee.
    """
    today = date.today()
    start_time = datetime.combine(today, datetime.min.time()).replace(hour=3)
    end_hour = 12 if window == "noon" else 15
    end_time = datetime.combine(today, datetime.min.time()).replace(hour=end_hour)

    raw = _fetch_midday_raw()
    if raw.empty:
        return pd.DataFrame()

    # Convert CompleteDate to datetime, drop parse failures
    raw["CompleteDate"] = pd.to_datetime(raw["CompleteDate"], errors="coerce")
    raw = raw.dropna(subset=["CompleteDate"])

    # Filter to time window (inclusive both ends)
    mask = (raw["CompleteDate"] >= start_time) & (raw["CompleteDate"] <= end_time)
    filtered = raw[mask].copy()

    if filtered.empty:
        return pd.DataFrame()

    # Aggregate per employee: unique cases and total duration (hours)
    filtered["Duration"] = pd.to_numeric(filtered["Duration"], errors="coerce").fillna(0)
    agg = filtered.groupby(["CompletedBy", "Name"]).agg(
        Cases=("CaseNumber", "nunique"),
        Tasks_Completed=("CaseNumber", "count"),
        Total_Duration_Hours=("Duration", lambda x: round(x.sum(), 2)),
    ).reset_index()

    # Cast CompletedBy to int for merge compatibility
    agg["CompletedBy"] = pd.to_numeric(agg["CompletedBy"], errors="coerce").fillna(0).astype(int)

    # Join with employee lookup to get Team
    lookup = load_employee_lookup()
    agg = agg.merge(
        lookup[["Employee ID", "Team"]],
        left_on="CompletedBy",
        right_on="Employee ID",
        how="left",
    )
    agg["Team"] = agg["Team"].fillna("Unknown")

    # Exclude z_Not On Report
    agg = agg[agg["Team"] != "z_Not On Report"]

    return agg[["Team", "Name", "Cases", "Tasks_Completed", "Total_Duration_Hours"]].sort_values(["Team", "Name"]).reset_index(drop=True)


# ─────────────────────────────────────────────
# Full Upload Orchestration
# ─────────────────────────────────────────────

def run_full_upload(gusto_bytes: bytes, filename: str) -> dict:
    """
    Orchestrate the full upload pipeline (Stages 1–4).
    Returns a dict with status and row counts.
    """
    from dashboard.data.efficiency_store import load_daily, save_daily, save_aggregated

    start_date, end_date = parse_gusto_filename(filename)
    logger.info(f"Processing Gusto upload: {filename} ({start_date} to {end_date})")

    # Stage 1: Task data from SQL
    raw_tasks = _fetch_tasks(start_date, end_date)
    task_df = stage1_task_processing(raw_tasks, start_date, end_date)

    # Stage 2: Gusto hours
    gusto_df = stage2_gusto_processing(gusto_bytes, filename)

    # Stage 3: Combine
    daily_new = stage3_combine(task_df, gusto_df)

    # Merge with existing daily data (replace rows for same date)
    existing = load_daily()
    if not existing.empty and not daily_new.empty:
        dates_to_replace = daily_new["Date"].unique().tolist()
        existing = existing[~existing["Date"].isin(dates_to_replace)]
        combined = pd.concat([existing, daily_new], ignore_index=True)
    elif not daily_new.empty:
        combined = daily_new
    else:
        combined = existing

    # Sort: Date desc, EmployeeID asc
    if not combined.empty:
        combined = combined.sort_values(["Date", "EmployeeID"], ascending=[False, True]).reset_index(drop=True)

    save_daily(combined)

    # Stage 4: Aggregated view from all historical data
    agg_df = stage4_aggregated(combined)
    save_aggregated(agg_df)

    return {
        "status": "ok",
        "date_range": f"{start_date} to {end_date}",
        "new_rows": len(daily_new),
        "total_daily_rows": len(combined),
        "aggregated_rows": len(agg_df),
    }
