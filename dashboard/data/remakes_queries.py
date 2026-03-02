"""Remakes dashboard data queries and cache management."""
import sys
import logging
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Optional

import pandas as pd
import pyodbc

# Add project root to path so we can import src modules
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.db_handler import get_sql_server_credentials
from dashboard.data.cache import cache

logger = logging.getLogger("dashboard.data.remakes")

_remakes_last_refresh: Optional[datetime] = None
_NOTES_PATH = Path(__file__).parent.parent.parent / "User_Inputs" / "remake_notes.csv"
_EMPLOYEE_LKUPS_PATH = Path(__file__).parent.parent.parent / "User_Inputs" / "employee_lkups.csv"

# ─── SQL Queries ──────────────────────────────────────────────────────────────

_ALL_REMAKES_SQL = """
SELECT
    main.CaseID                   AS MainCaseID,
    linked.CaseID                 AS OG_CaseID,
    main.DateIn                   AS DateIn_TIME,
    CAST(linked.ShipDate AS DATE) AS OG_ShipDate,
    CAST(linked.DueDate  AS DATE) AS OG_DueDate,
    linked.CaseNumber             AS OG_CaseNumber,
    main.CaseNumber               AS MainCaseNumber,
    CAST(main.DateIn   AS DATE)   AS DateIn,
    CAST(main.ShipDate AS DATE)   AS ShipDate,
    cust.PracticeName,
    main.TotalCharge,
    main.RemakeReason,
    main.Remake,
    main.RemakeDiscount,
    main.[Status],
    ISNULL(T2.Cases,   0)         AS TotalCases_90Days,
    ISNULL(T2.Remakes, 0)         AS TotalRemakes_90Days,
    cust.SalesPerson,
    topProduct.Description        AS Product
FROM dbo.CaseLinks AS links
INNER JOIN dbo.Cases     AS main   ON links.CaseID     = main.CaseID
INNER JOIN dbo.Cases     AS linked ON links.LinkCaseID = linked.CaseID
INNER JOIN dbo.Customers AS cust   ON main.CustomerID  = cust.CustomerID
LEFT JOIN (
    SELECT
        cu.PracticeName,
        COUNT(ca.CaseID) AS Cases,
        COUNT(CASE WHEN NULLIF(LTRIM(RTRIM(ca.Remake)), '') IS NOT NULL THEN 1 END) AS Remakes
    FROM dbo.Cases     AS ca
    INNER JOIN dbo.Customers AS cu ON ca.CustomerID = cu.CustomerID
    WHERE ca.InvoiceDate >= DATEADD(DAY, -91, CAST(GETDATE() AS DATE))
    GROUP BY cu.PracticeName
) AS T2 ON cust.PracticeName = T2.PracticeName
OUTER APPLY (
    SELECT TOP 1 p.Description
    FROM dbo.CaseProducts AS cp
    INNER JOIN dbo.Products AS p ON cp.ProductID = p.ProductID
    WHERE cp.CaseID = linked.CaseID
      AND p.Description NOT LIKE '%Rush%'
    ORDER BY cp.UnitPrice DESC
) AS topProduct
WHERE links.Notes LIKE '%Remake Of%'
  AND main.DateIn >= DATEADD(DAY, -180, CAST(GETDATE() AS DATE))
  AND main.[Status] IN ('In Production', 'Invoiced', 'On Hold')
ORDER BY main.DateIn DESC
"""

_CASE_TASKS_SQL = """
WITH remake_ids AS (
    SELECT main.CaseID
    FROM dbo.CaseLinks AS links
    INNER JOIN dbo.Cases AS main ON links.CaseID = main.CaseID
    WHERE links.Notes LIKE '%Remake Of%'
      AND main.DateIn >= DATEADD(DAY, -180, CAST(GETDATE() AS DATE))
      AND main.[Status] IN ('In Production', 'Invoiced', 'On Hold')
    UNION
    SELECT linked.CaseID
    FROM dbo.CaseLinks AS links
    INNER JOIN dbo.Cases AS main   ON links.CaseID     = main.CaseID
    INNER JOIN dbo.Cases AS linked ON links.LinkCaseID = linked.CaseID
    WHERE links.Notes LIKE '%Remake Of%'
      AND main.DateIn >= DATEADD(DAY, -180, CAST(GETDATE() AS DATE))
      AND main.[Status] IN ('In Production', 'Invoiced', 'On Hold')
)
SELECT cth.CaseID, cth.Task, cth.CompletedBy, cth.CompleteDate
FROM dbo.CaseTasksHistory AS cth
INNER JOIN remake_ids ON cth.CaseID = remake_ids.CaseID
ORDER BY cth.CaseID, cth.CompleteDate ASC
"""

_CALL_NOTES_SQL = """
WITH remake_ids AS (
    SELECT main.CaseID
    FROM dbo.CaseLinks AS links
    INNER JOIN dbo.Cases AS main ON links.CaseID = main.CaseID
    WHERE links.Notes LIKE '%Remake Of%'
      AND main.DateIn >= DATEADD(DAY, -180, CAST(GETDATE() AS DATE))
      AND main.[Status] IN ('In Production', 'Invoiced', 'On Hold')
    UNION
    SELECT linked.CaseID
    FROM dbo.CaseLinks AS links
    INNER JOIN dbo.Cases AS main   ON links.CaseID     = main.CaseID
    INNER JOIN dbo.Cases AS linked ON links.LinkCaseID = linked.CaseID
    WHERE links.Notes LIKE '%Remake Of%'
      AND main.DateIn >= DATEADD(DAY, -180, CAST(GETDATE() AS DATE))
      AND main.[Status] IN ('In Production', 'Invoiced', 'On Hold')
)
SELECT cn.Note, cn.UserID, chc.AnchorCaseID, chc.LinkCaseID
FROM dbo.CallNotes AS cn
INNER JOIN dbo.CaseHistory_Calls AS chc ON cn.CallID = chc.RedCallID
WHERE chc.AnchorCaseID IN (SELECT CaseID FROM remake_ids)
   OR chc.LinkCaseID   IN (SELECT CaseID FROM remake_ids)
ORDER BY chc.AnchorCaseID, chc.LinkCaseID
"""

_REVENUE_BY_DAY_SQL = """
SELECT
    CAST(InvoiceDate AS DATE)             AS InvoiceDate,
    SUM(TaxableAmount + NonTaxableAmount) AS Revenue
FROM dbo.Cases
WHERE [Status] NOT IN ('Cancelled', 'Submitted', 'Sent for TryIn')
  AND Deleted = 0
  AND [Type] = 'D'
  AND InvoiceDate >= DATEADD(DAY, -180, CAST(GETDATE() AS DATE))
  AND InvoiceDate <  DATEADD(DAY,   1, CAST(GETDATE() AS DATE))
GROUP BY CAST(InvoiceDate AS DATE)
ORDER BY InvoiceDate ASC
"""

# ─── Query functions ──────────────────────────────────────────────────────────

def get_all_remakes(conn) -> pd.DataFrame:
    return pd.read_sql(_ALL_REMAKES_SQL, conn)


def get_case_tasks(conn) -> pd.DataFrame:
    return pd.read_sql(_CASE_TASKS_SQL, conn)


def get_call_notes(conn) -> pd.DataFrame:
    return pd.read_sql(_CALL_NOTES_SQL, conn)


def get_revenue_by_day(conn) -> pd.DataFrame:
    return pd.read_sql(_REVENUE_BY_DAY_SQL, conn)


# ─── Employee lookup helpers ──────────────────────────────────────────────────

def _load_employee_id_map() -> dict:
    """Load employee_lkups.csv, return {Employee ID (int) -> MT Name} dict."""
    try:
        df = pd.read_csv(_EMPLOYEE_LKUPS_PATH, dtype=str)
        result = {}
        for _, row in df.iterrows():
            try:
                emp_id = int(row["Employee ID"])
                result[emp_id] = row.get("MT Name", "")
            except (ValueError, KeyError):
                continue
        return result
    except Exception as e:
        logger.warning(f"Failed to load employee_lkups.csv: {e}")
        return {}


def _apply_employee_names(df: pd.DataFrame, id_col: str, name_col: str) -> pd.DataFrame:
    """Map IDs in id_col to employee names, write to name_col."""
    if df.empty or id_col not in df.columns:
        return df
    emp_map = _load_employee_id_map()

    def resolve(v):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return ""
        s = str(v).strip()
        try:
            return emp_map.get(int(float(s)), s)
        except (ValueError, TypeError):
            return s

    df = df.copy()
    df[name_col] = df[id_col].apply(resolve)
    return df


# ─── Notes storage ────────────────────────────────────────────────────────────

def load_remake_notes() -> pd.DataFrame:
    """Load User_Inputs/remake_notes.csv.
    Returns empty DataFrame with expected columns if file missing."""
    if _NOTES_PATH.exists():
        try:
            return pd.read_csv(_NOTES_PATH, dtype=str)
        except Exception as e:
            logger.warning(f"Failed to read remake_notes.csv: {e}")
    return pd.DataFrame(columns=["MainCaseNumber", "Note", "LastUpdated"])


def save_remake_note(case_number: str, note_text: str) -> None:
    """Upsert a note by MainCaseNumber. Creates file if needed."""
    existing = load_remake_notes()
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if not existing.empty and case_number in existing["MainCaseNumber"].values:
        existing.loc[existing["MainCaseNumber"] == case_number, "Note"] = note_text
        existing.loc[existing["MainCaseNumber"] == case_number, "LastUpdated"] = now_str
    else:
        new_row = pd.DataFrame([{
            "MainCaseNumber": case_number,
            "Note": note_text,
            "LastUpdated": now_str,
        }])
        existing = pd.concat([existing, new_row], ignore_index=True)
    _NOTES_PATH.parent.mkdir(parents=True, exist_ok=True)
    existing.to_csv(_NOTES_PATH, index=False)


# ─── Cache helpers ────────────────────────────────────────────────────────────

async def refresh_remakes_cache(conn) -> dict:
    """Run all 4 queries, apply employee name mapping, store in cache.
    Sets _remakes_last_refresh. Returns row count dict."""
    global _remakes_last_refresh

    all_df = get_all_remakes(conn)
    tasks_df = get_case_tasks(conn)
    notes_df = get_call_notes(conn)
    revenue_df = get_revenue_by_day(conn)

    tasks_df = _apply_employee_names(tasks_df, "CompletedBy", "CompletedByName")
    notes_df = _apply_employee_names(notes_df, "UserID", "UserName")

    await cache.set("remakes_all", all_df)
    await cache.set("remakes_tasks", tasks_df)
    await cache.set("remakes_notes_text", notes_df)
    await cache.set("remakes_revenue", revenue_df)

    _remakes_last_refresh = datetime.now()

    return {
        "all_rows": len(all_df),
        "tasks_rows": len(tasks_df),
        "notes_rows": len(notes_df),
        "revenue_rows": len(revenue_df),
    }


def get_cached_remakes() -> dict:
    """Return all cached remakes datasets + last_refresh timestamp."""
    return {
        "all": cache.get_sync("remakes_all"),
        "tasks": cache.get_sync("remakes_tasks"),
        "notes_text": cache.get_sync("remakes_notes_text"),
        "revenue": cache.get_sync("remakes_revenue"),
        "last_refresh": _remakes_last_refresh,
    }


def get_db_connection():
    """pyodbc connection using same credentials as db_handler.py."""
    creds = get_sql_server_credentials()
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={creds['SERVER']};"
        f"DATABASE={creds['DATABASE']};"
        f"UID={creds['USERNAME']};"
        f"PWD={creds['PASSWORD']}"
    )
    return pyodbc.connect(conn_str)


# ─── Week bounds helper ───────────────────────────────────────────────────────

def get_current_week_bounds() -> tuple:
    """Return (week_start, week_end) for the current Tue–Mon week."""
    today = date.today()
    days_since_tuesday = (today.weekday() - 1) % 7
    week_start = today - timedelta(days=days_since_tuesday)
    week_end = week_start + timedelta(days=6)
    return week_start, week_end
