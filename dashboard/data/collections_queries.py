"""Past Due Collections data queries, cache, and CSV persistence."""
import sys
import asyncio
import csv
import logging
import time
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
    cu.CustomerID,
    cu.PracticeName,
    cu.DentalGroup,
    CONCAT(cu.FirstName, ' ', cu.LastName) AS FullName,
    cu.OfficePhone,
    cu.Email,
    cu.SalesPerson,
    cu.PastDue30,
    cu.PastDue60,
    cu.PastDue90,
    cu.PastDueOver90,
    cu.CurrentBalance,
    cu.TotalBalance
FROM dbo.customers AS cu
WHERE cu.PastDue90 > 0
ORDER BY cu.PastDue90 DESC;
"""

_OPEN_CASES_SQL = """
SELECT
    cu.CustomerID,
    ca.CaseID,
    ca.CaseNumber,
    ca.PatientFirstName,
    ca.PatientLastName,
    ca.Status,
    CAST(ca.DueDate AS DATE) AS DueDate,
    CAST(ca.DateEntered AS DATE) AS DateEntered
FROM dbo.customers AS cu
INNER JOIN dbo.cases AS ca
    ON cu.CustomerID = ca.CustomerID
WHERE cu.PastDue90 > 0
  AND ca.Status IN ('In Production')
  AND ca.Deleted = 0
ORDER BY cu.CustomerID, ca.DateEntered ASC;
"""


# ─── Query functions ──────────────────────────────────────────────────────────

_BALANCE_COLS = ["PastDue30", "PastDue60", "PastDue90", "PastDueOver90",
                 "CurrentBalance", "TotalBalance"]


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
    Returns empty DataFrame with expected columns if file missing."""
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
) -> str:
    """Partial upsert by CustomerID. Only non-None fields are written;
    missing fields leave existing values alone. Returns the timestamp
    used for LastContacted (empty string if not updated)."""
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
