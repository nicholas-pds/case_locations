"""Remakes dashboard data queries and cache management."""
import sys
import asyncio
import logging
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Optional
from concurrent.futures import ThreadPoolExecutor

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
LEFT JOIN (
    SELECT cp.CaseID, p.Description,
           ROW_NUMBER() OVER (PARTITION BY cp.CaseID ORDER BY cp.UnitPrice DESC) AS rn
    FROM dbo.CaseProducts AS cp
    INNER JOIN dbo.Products AS p ON cp.ProductID = p.ProductID
    WHERE p.Description NOT LIKE '%Rush%'
) AS topProduct ON topProduct.CaseID = linked.CaseID AND topProduct.rn = 1
WHERE links.Notes LIKE '%Remake Of%'
  AND main.DateIn >= DATEADD(DAY, -365, CAST(GETDATE() AS DATE))
  AND main.[Status] IN ('In Production', 'Invoiced', 'On Hold')
ORDER BY main.DateIn DESC
"""

_REVENUE_BY_DAY_SQL = """
SELECT
    CAST(InvoiceDate AS DATE)             AS InvoiceDate,
    SUM(TaxableAmount + NonTaxableAmount) AS Revenue
FROM dbo.Cases
WHERE [Status] NOT IN ('Cancelled', 'Submitted', 'Sent for TryIn')
  AND Deleted = 0
  AND [Type] = 'D'
  AND InvoiceDate >= DATEADD(DAY, -365, CAST(GETDATE() AS DATE))
  AND InvoiceDate <  DATEADD(DAY,   1, CAST(GETDATE() AS DATE))
GROUP BY CAST(InvoiceDate AS DATE)
ORDER BY InvoiceDate ASC
"""

_CASE_TASKS_SQL = """
WITH RemakeCases AS (
    SELECT main.CaseID AS MainCaseID, linked.CaseID AS OG_CaseID
    FROM dbo.CaseLinks AS links
    INNER JOIN dbo.Cases AS main   ON links.CaseID     = main.CaseID
    INNER JOIN dbo.Cases AS linked ON links.LinkCaseID = linked.CaseID
    WHERE links.Notes LIKE '%Remake Of%'
      AND main.DateIn >= DATEADD(DAY, -365, CAST(GETDATE() AS DATE))
      AND main.[Status] IN ('In Production', 'Invoiced', 'On Hold')
)
SELECT th.CaseID, th.Task, th.CompletedBy, CAST(th.CompleteDate AS DATE) AS CompleteDate
FROM dbo.CaseTasksHistory AS th
WHERE th.CaseID IN (
    SELECT MainCaseID FROM RemakeCases
    UNION
    SELECT OG_CaseID  FROM RemakeCases
)
ORDER BY th.CaseID, th.CompleteDate ASC
"""

_CASE_DOCUMENTS_SQL = """
WITH RemakeCases AS (
    SELECT DISTINCT main.CaseID
    FROM dbo.CaseLinks AS links
    INNER JOIN dbo.Cases AS main   ON links.CaseID       = main.CaseID
    INNER JOIN dbo.Cases AS linked ON links.LinkCaseID = linked.CaseID
    WHERE links.Notes LIKE N'%Remake Of%'
    AND main.DateIn >= DATEADD(DAY, -365, GETDATE())
    AND main.[Status] IN (N'In Production', N'Invoiced', N'On Hold')
    UNION
    SELECT DISTINCT linked.CaseID
    FROM dbo.CaseLinks AS links
    INNER JOIN dbo.Cases AS main   ON links.CaseID     = main.CaseID
    INNER JOIN dbo.Cases AS linked ON links.LinkCaseID = linked.CaseID
    WHERE links.Notes LIKE N'%Remake Of%'
    AND main.DateIn >= DATEADD(DAY, -365, GETDATE())
    AND main.[Status] IN (N'In Production', N'Invoiced', N'On Hold')
)
SELECT cd.CaseID, cd.FilePath, cd.SourceFileName, cd.CreateDate
FROM dbo.CaseDocuments AS cd
INNER JOIN RemakeCases rc ON rc.CaseID = cd.CaseID
ORDER BY cd.CaseID, cd.CreateDate
"""

_CALL_NOTES_SQL = """
WITH RemakeCases AS (
    SELECT main.CaseID AS MainCaseID, linked.CaseID AS OG_CaseID
    FROM dbo.CaseLinks AS links
    INNER JOIN dbo.Cases AS main   ON links.CaseID     = main.CaseID
    INNER JOIN dbo.Cases AS linked ON links.LinkCaseID = linked.CaseID
    WHERE links.Notes LIKE '%Remake Of%'
      AND main.DateIn >= DATEADD(DAY, -365, CAST(GETDATE() AS DATE))
      AND main.[Status] IN ('In Production', 'Invoiced', 'On Hold')
), AllCaseIDs AS (
    SELECT MainCaseID AS CaseID FROM RemakeCases
    UNION
    SELECT OG_CaseID  AS CaseID FROM RemakeCases
)
SELECT cn.Note, cn.UserID, cn.[Date] AS CallDate, chc.AnchorCaseID, chc.LinkCaseID
FROM dbo.CallNotes           AS cn
INNER JOIN dbo.CaseHistory_Calls AS chc ON cn.CallID = chc.RefCallID
WHERE chc.AnchorCaseID IN (SELECT CaseID FROM AllCaseIDs)
   OR chc.LinkCaseID   IN (SELECT CaseID FROM AllCaseIDs)
"""

# ─── Query functions ──────────────────────────────────────────────────────────

def get_all_remakes(conn) -> pd.DataFrame:
    return pd.read_sql(_ALL_REMAKES_SQL, conn)


def get_revenue_by_day(conn) -> pd.DataFrame:
    return pd.read_sql(_REVENUE_BY_DAY_SQL, conn)


def get_case_tasks(conn) -> pd.DataFrame:
    return pd.read_sql(_CASE_TASKS_SQL, conn)


def get_call_notes(conn) -> pd.DataFrame:
    return pd.read_sql(_CALL_NOTES_SQL, conn)


def get_case_documents(conn) -> pd.DataFrame:
    return pd.read_sql(_CASE_DOCUMENTS_SQL, conn)


def get_tasks_for_case(conn, main_id: int, og_id: int) -> pd.DataFrame:
    sql = """
    SELECT CaseID, Task, CompletedBy, CompleteDate
    FROM dbo.CaseTasksHistory
    WHERE CaseID IN (?, ?)
    ORDER BY CompleteDate ASC
    """
    return pd.read_sql(sql, conn, params=[main_id, og_id])


def get_notes_for_case(conn, main_id: int, og_id: int) -> pd.DataFrame:
    sql = """
    SELECT cn.Note, cn.UserID, cn.[Date] AS CallDate, chc.AnchorCaseID, chc.LinkCaseID
    FROM dbo.CallNotes AS cn
    INNER JOIN dbo.CaseHistory_Calls AS chc ON cn.CallID = chc.RefCallID
    WHERE chc.AnchorCaseID IN (?, ?)
       OR chc.LinkCaseID   IN (?, ?)
    """
    return pd.read_sql(sql, conn, params=[main_id, og_id, main_id, og_id])


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

async def refresh_remakes_cache() -> dict:
    """Run all 4 queries in parallel, store in cache.
    Each query opens its own connection. Sets _remakes_last_refresh."""
    global _remakes_last_refresh
    loop = asyncio.get_event_loop()

    def _run(fn):
        conn = get_db_connection()
        try:
            return fn(conn)
        finally:
            conn.close()

    with ThreadPoolExecutor(max_workers=5) as pool:
        all_df, revenue_df, tasks_df, notes_df, docs_df = await asyncio.gather(
            loop.run_in_executor(pool, _run, get_all_remakes),
            loop.run_in_executor(pool, _run, get_revenue_by_day),
            loop.run_in_executor(pool, _run, get_case_tasks),
            loop.run_in_executor(pool, _run, get_call_notes),
            loop.run_in_executor(pool, _run, get_case_documents),
        )

    tasks_df = _apply_employee_names(tasks_df, "CompletedBy", "CompletedByName")
    notes_df = _apply_employee_names(notes_df, "UserID", "UserName")

    await cache.set("remakes_all", all_df)
    await cache.set("remakes_revenue", revenue_df)
    await cache.set("remakes_tasks", tasks_df)
    await cache.set("remakes_notes_text", notes_df)
    await cache.set("remakes_documents", docs_df)
    _remakes_last_refresh = datetime.now()

    return {
        "all_rows": len(all_df),
        "revenue_rows": len(revenue_df),
        "tasks_rows": len(tasks_df),
        "notes_rows": len(notes_df),
        "docs_rows": len(docs_df),
    }


def get_cached_remakes() -> dict:
    """Return cached remakes datasets + last_refresh timestamp."""
    return {
        "all": cache.get_sync("remakes_all"),
        "revenue": cache.get_sync("remakes_revenue"),
        "tasks": cache.get_sync("remakes_tasks"),
        "notes_text": cache.get_sync("remakes_notes_text"),
        "documents": cache.get_sync("remakes_documents"),
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
    """Return (week_start, week_end) for the current Mon–Fri work week.
    On Sat/Sun, returns the prior week's Mon–Fri."""
    today = date.today()
    dow = today.weekday()          # 0=Mon … 4=Fri, 5=Sat, 6=Sun
    days_to_mon = dow if dow <= 4 else dow   # Sat→5, Sun→6 (back to prior Mon)
    week_start = today - timedelta(days=days_to_mon)
    week_end   = week_start + timedelta(days=4)   # Friday
    return week_start, week_end
