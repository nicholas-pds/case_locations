"""Past Due Collections data queries, cache, and CSV persistence."""
import sys
import asyncio
import csv
import logging
import time
from io import BytesIO
from pathlib import Path
from datetime import datetime
from typing import Optional
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import pyodbc

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.db_handler import get_sql_server_credentials
from dashboard.data.cache import cache

logger = logging.getLogger("dashboard.data.collections")

_collections_last_refresh: Optional[datetime] = None
_LOG_PATH = Path(__file__).parent.parent.parent / "User_Inputs" / "collections_log.csv"
_LOG_COLUMNS = [
    "CustomerID", "LastContacted", "Outcome", "Notes",
    "WhoLogged", "Completed", "LastUpdated",
]
_collections_lock = asyncio.Lock()

# ─── SQL Queries ──────────────────────────────────────────────────────────────

_ACCOUNTS_SQL = """
SELECT
    c.LabName,
    c.CustomerID,
    c.PracticeName,
    c.DentalGroup,
    ISNULL(c.FirstName, '') + ' ' + ISNULL(c.LastName, '')        AS FullName,
    c.OfficePhone,
    c.BillEmail                                                    AS Email,
    c.SalesPerson,
    c.LastPaymentDate,
    c.LastPaymentAmount,
    CAST(c.UnAppliedPayments + c.UnAppliedCredits AS DECIMAL(12,2)) AS UnApplied,
    CAST(ISNULL(c.ThisPeriodCharges, 0)           AS DECIMAL(12,2)) AS ThisPeriod,
    CAST(c.CurrentBalance                         AS DECIMAL(12,2)) AS CurrentBalance,
    CAST(c.PastDue30                              AS DECIMAL(12,2)) AS PastDue30,
    CAST(c.PastDue60                              AS DECIMAL(12,2)) AS PastDue60,
    CAST(c.PastDue90                              AS DECIMAL(12,2)) AS PastDue90,
    CAST(c.PastDueOver90                          AS DECIMAL(12,2)) AS PastDueOver90,
    CAST(c.PastDue30 + c.PastDue60 + c.PastDue90
         + c.PastDueOver90                        AS DECIMAL(12,2)) AS TotalPastDue,
    CAST(c.TotalBalance                           AS DECIMAL(12,2)) AS TotalBalance,
    CASE
        WHEN c.IsOnCOD = 1      THEN 'COD'
        WHEN c.OnCreditHold = 1 THEN 'Credit Hold'
        WHEN c.InCollection = 1 THEN 'In Collection'
        ELSE ''
    END                                                             AS AccountFlag
FROM dbo.Customers c
WHERE c.Deleted = 0
  AND (c.PastDue90 + c.PastDueOver90) > 0
  AND ISNULL(c.DentalGroup, '') <> 'Retain'
  AND c.LabName = 'PartnersDental'
ORDER BY (c.PastDue30 + c.PastDue60 + c.PastDue90 + c.PastDueOver90) DESC;
"""

_OPEN_CASES_SQL = """
SELECT
    cu.CustomerID,
    ca.CaseID,
    ca.CaseNumber,
    ca.PatientFirst  AS PatientFirstName,
    ca.PatientLast   AS PatientLastName,
    ca.Status,
    CAST(ca.DueDate AS DATE) AS DueDate,
    CAST(ca.DateIn  AS DATE) AS DateEntered
FROM dbo.customers AS cu
INNER JOIN dbo.cases AS ca
    ON cu.CustomerID = ca.CustomerID
WHERE ca.CustomerID IN (
    SELECT CustomerID FROM dbo.Customers
    WHERE Deleted = 0
      AND (PastDue90 + PastDueOver90) > 0
      AND ISNULL(DentalGroup, '') <> 'Retain'
)
  AND ca.Status IN ('In Production', 'On Hold')
  AND ca.Deleted = 0
  AND ca.LabName = 'PartnersDental'
ORDER BY cu.CustomerID, ca.DateIn ASC;
"""


# ─── Query functions ──────────────────────────────────────────────────────────

_BALANCE_COLS = [
    "PastDue30", "PastDue60", "PastDue90", "PastDueOver90",
    "CurrentBalance", "TotalPastDue", "TotalBalance",
    "UnApplied", "ThisPeriod", "LastPaymentAmount",
]


def get_collections_accounts(conn) -> pd.DataFrame:
    df = pd.read_sql(_ACCOUNTS_SQL, conn)
    if df.empty:
        return df
    df["CustomerID"] = df["CustomerID"].astype(str)
    for col in _BALANCE_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0).astype(float)
    return df


def get_collections_cases(conn) -> pd.DataFrame:
    df = pd.read_sql(_OPEN_CASES_SQL, conn)
    if df.empty:
        return df
    df["CustomerID"] = df["CustomerID"].astype(str)
    return df


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


# ─── Call-log CSV persistence ────────────────────────────────────────────────

def load_collections_log() -> pd.DataFrame:
    """Load User_Inputs/collections_log.csv.
    Returns empty DataFrame with expected columns if file missing.
    Data is never mutated automatically — resolved entries persist until the
    user explicitly unmarks them."""
    if _LOG_PATH.exists():
        try:
            df = pd.read_csv(_LOG_PATH, dtype=str, quoting=csv.QUOTE_ALL,
                             keep_default_na=False)
            for col in _LOG_COLUMNS:
                if col not in df.columns:
                    df[col] = ""
            df["CustomerID"] = df["CustomerID"].astype(str)
            df["Completed"] = df["Completed"].replace("", "0").astype(str)
            return df[_LOG_COLUMNS]
        except Exception as e:
            logger.warning(f"Failed to read collections_log.csv: {e}")
    return pd.DataFrame(columns=_LOG_COLUMNS)


def _write_log(df: pd.DataFrame) -> None:
    _LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(_LOG_PATH, index=False, quoting=csv.QUOTE_ALL)


def save_collection_entry(
    customer_id: str,
    *,
    outcome: Optional[str] = None,
    notes: Optional[str] = None,
    who_logged: Optional[str] = None,
    mark_contacted: bool = False,
    clear_contacted: bool = False,
) -> str:
    """Partial upsert by CustomerID. Only non-None fields are written;
    missing fields leave existing values alone. Returns the timestamp
    used for LastContacted (empty string if not updated or cleared)."""
    customer_id = str(customer_id)
    existing = load_collections_log()
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    contacted_ts = now_str if mark_contacted else ""

    if not existing.empty and customer_id in existing["CustomerID"].values:
        mask = existing["CustomerID"] == customer_id
        if outcome is not None:
            existing.loc[mask, "Outcome"] = outcome
        if notes is not None:
            existing.loc[mask, "Notes"] = notes
        if who_logged is not None:
            existing.loc[mask, "WhoLogged"] = who_logged
        if mark_contacted:
            existing.loc[mask, "LastContacted"] = now_str
        elif clear_contacted:
            existing.loc[mask, "LastContacted"] = ""
        existing.loc[mask, "LastUpdated"] = now_str
    else:
        new_row = {
            "CustomerID": customer_id,
            "LastContacted": now_str if mark_contacted else "",
            "Outcome": outcome or "",
            "Notes": notes or "",
            "WhoLogged": who_logged or "",
            "Completed": "0",
            "LastUpdated": now_str,
        }
        existing = pd.concat([existing, pd.DataFrame([new_row])], ignore_index=True)

    _write_log(existing)
    return contacted_ts


def save_collection_completed(customer_id: str, completed: bool) -> None:
    """Upsert the Completed flag by CustomerID."""
    customer_id = str(customer_id)
    existing = load_collections_log()
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    val = "1" if completed else "0"

    if not existing.empty and customer_id in existing["CustomerID"].values:
        mask = existing["CustomerID"] == customer_id
        existing.loc[mask, "Completed"] = val
        existing.loc[mask, "LastUpdated"] = now_str
    else:
        new_row = {
            "CustomerID": customer_id,
            "LastContacted": "",
            "Outcome": "",
            "Notes": "",
            "WhoLogged": "",
            "Completed": val,
            "LastUpdated": now_str,
        }
        existing = pd.concat([existing, pd.DataFrame([new_row])], ignore_index=True)

    _write_log(existing)


# ─── Cache helpers ────────────────────────────────────────────────────────────

async def refresh_collections_cache() -> dict:
    """Run both SQL queries in parallel, store in cache."""
    global _collections_last_refresh
    loop = asyncio.get_running_loop()

    def _run(fn, max_retries: int = 3, base_delay: float = 0.5):
        last_exc = None
        for attempt in range(max_retries):
            conn = get_db_connection()
            try:
                return fn(conn)
            except pyodbc.Error as e:
                if e.args and e.args[0] == "40001":  # deadlock victim
                    last_exc = e
                    conn.close()
                    time.sleep(base_delay * (attempt + 1))
                    continue
                raise
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
        raise last_exc

    with ThreadPoolExecutor(max_workers=2) as pool:
        accounts_df, cases_df = await asyncio.gather(
            loop.run_in_executor(pool, _run, get_collections_accounts),
            loop.run_in_executor(pool, _run, get_collections_cases),
        )

    await cache.set("collections_accounts", accounts_df)
    await cache.set("collections_cases", cases_df)
    _collections_last_refresh = datetime.now()

    return {
        "account_rows": len(accounts_df),
        "case_rows": len(cases_df),
    }


def get_cached_collections() -> dict:
    """Return cached datasets + last_refresh timestamp."""
    return {
        "accounts": cache.get_sync("collections_accounts"),
        "cases": cache.get_sync("collections_cases"),
        "last_refresh": _collections_last_refresh,
    }


# ─── Excel export ─────────────────────────────────────────────────────────────

_EXPORT_COLUMNS = [
    "PracticeName", "DentalGroup", "FullName", "OfficePhone", "Email",
    "SalesPerson", "LastPaymentDate", "LastPaymentAmount",
    "UnApplied", "ThisPeriod", "CurrentBalance",
    "PastDue30", "PastDue60", "PastDue90", "PastDueOver90",
    "TotalPastDue", "TotalBalance",
    "AccountFlag", "OpenCaseCount",
    "LastContacted", "Outcome", "WhoLogged", "Notes", "Completed",
]
_MONEY_COLS = {
    "UnApplied", "ThisPeriod", "CurrentBalance",
    "PastDue30", "PastDue60", "PastDue90", "PastDueOver90",
    "TotalPastDue", "TotalBalance", "LastPaymentAmount",
}


def build_export_workbook(s1: pd.DataFrame, s2: pd.DataFrame,
                          s3: pd.DataFrame, log_dict: dict) -> BytesIO:
    """Return BytesIO XLSX with three sheets — one per collection bucket."""

    def _merge(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        for col, key in [("LastContacted", "LastContacted"), ("Outcome", "Outcome"),
                         ("WhoLogged", "WhoLogged"), ("Notes", "Notes")]:
            df[col] = df["CustomerID"].map(
                lambda cid, k=key: log_dict.get(str(cid), {}).get(k, ""))
        df["Completed"] = df["CustomerID"].map(
            lambda cid: "Yes" if log_dict.get(str(cid), {}).get("Completed") == "1" else "")
        cols = [c for c in _EXPORT_COLUMNS if c in df.columns]
        return df[cols]

    buf = BytesIO()
    sheets = [
        ("General Aging", _merge(s1) if not s1.empty else pd.DataFrame(columns=_EXPORT_COLUMNS)),
        ("Small Balances", _merge(s2) if not s2.empty else pd.DataFrame(columns=_EXPORT_COLUMNS)),
        ("Smile Doctors", _merge(s3) if not s3.empty else pd.DataFrame(columns=_EXPORT_COLUMNS)),
    ]

    with pd.ExcelWriter(buf, engine="openpyxl") as xw:
        for sheet_name, df in sheets:
            df.to_excel(xw, sheet_name=sheet_name, index=False)
            ws = xw.sheets[sheet_name]
            ws.freeze_panes = "A2"
            # Column widths + number format
            for col_idx, col_name in enumerate(_EXPORT_COLUMNS, start=1):
                if col_idx > ws.max_column:
                    break
                letter = ws.cell(row=1, column=col_idx).column_letter
                ws.column_dimensions[letter].width = 14 if col_name in _MONEY_COLS else 22
                if col_name in _MONEY_COLS:
                    for row_idx in range(2, ws.max_row + 1):
                        ws.cell(row=row_idx, column=col_idx).number_format = '"$"#,##0.00'
    buf.seek(0)
    return buf
