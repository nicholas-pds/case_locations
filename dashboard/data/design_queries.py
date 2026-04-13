"""Design task query: 3dd (Design) and 3dcf (Clean File) counts over last 6 business days,
split into Noon (3am–12pm) and 3PM (3am–3pm) time windows."""
import sys
import pandas as pd
from datetime import date, datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))
SQL_DIR = PROJECT_ROOT / "sql_query"

from src.db_handler import execute_sql_to_dataframe
from dashboard.data.efficiency_store import load_employee_lkups


def _last_n_business_days(n: int) -> list:
    """Return last n Mon-Fri dates (including today if weekday), desc order."""
    result, d = [], date.today()
    while len(result) < n:
        if d.weekday() < 5:  # 0=Mon, 4=Fri
            result.append(d)
        d -= timedelta(days=1)
    return result  # [today, yesterday, ...]


def _short(name: str) -> str:
    """Convert 'John Smith' → 'John S.' (matches JS shortName logic)."""
    parts = name.strip().split()
    if len(parts) < 2:
        return name
    return parts[0] + " " + parts[-1][0] + "."


def _build_by_emp(window_df: pd.DataFrame, task: str) -> list:
    """Return sorted [{name, count}] list for a given task within a windowed DataFrame."""
    task_df = window_df[window_df["Task"] == task].copy()
    # Deduplicate on (CaseNumber, Task) per employee — same counting logic as airway SQL
    task_df = task_df.drop_duplicates(subset=["CaseNumber", "Task", "emp_name"])
    grouped = task_df.groupby("emp_name").size().reset_index(name="count")
    result = [{"name": r["emp_name"], "count": int(r["count"])} for _, r in grouped.iterrows()]
    result.sort(key=lambda x: x["count"], reverse=True)
    return result


def fetch_design_tasks() -> tuple:
    """
    Execute design_tasks_export.sql, filter to last 6 business days,
    split each day into Noon (3am–12pm) and 3PM (3am–3pm) windows,
    resolve employee names, and return structured data for template.

    Returns (records, fetched_at) where:
      records = list of dicts, one per business day (desc: today first):
      {
        "date": "2026-04-09",
        "label": "Thu 4/9",
        "noon_dd_total": 12,  "noon_cf_total": 8,
        "noon_dd_by_emp": [...], "noon_cf_by_emp": [...],
        "pm3_dd_total": 15,   "pm3_cf_total": 10,
        "pm3_dd_by_emp": [...], "pm3_cf_by_emp": [...],
      }
      fetched_at = "2:34 PM" string
    """
    fetched_at = datetime.now().strftime("%I:%M %p").lstrip("0")

    df = execute_sql_to_dataframe(str(SQL_DIR / "design_tasks_export.sql"))
    if df.empty:
        return [], fetched_at

    # Build employee ID → short name map
    emp_df = load_employee_lkups()
    emp_map: dict[int, str] = {}
    if not emp_df.empty:
        for _, row in emp_df.iterrows():
            try:
                emp_map[int(row["Employee ID"])] = _short(str(row["MT Name"]))
            except (ValueError, TypeError):
                pass

    # Normalize column names
    df.columns = [c.strip() for c in df.columns]

    # Parse completeDate as full datetime for time-window filtering
    df["completeDate"] = pd.to_datetime(df["completeDate"])
    df["date_only"] = df["completeDate"].dt.date
    df["date_str"] = df["date_only"].astype(str)

    # Filter to last 6 business days
    biz_days = _last_n_business_days(6)
    biz_day_strs = {str(d) for d in biz_days}
    df = df[df["date_str"].isin(biz_day_strs)].copy()

    # Resolve employee name
    def _resolve(emp_id):
        try:
            return emp_map.get(int(emp_id), f"ID:{emp_id}")
        except (ValueError, TypeError):
            return str(emp_id)

    df["emp_name"] = df["Completed by name"].apply(_resolve)

    result = []
    for d in biz_days:
        date_str = str(d)
        day_df = df[df["date_str"] == date_str].copy()

        # Windows-safe label (avoids %-m which is Linux-only)
        label = d.strftime("%a") + " " + str(d.month) + "/" + str(d.day)

        # Time boundaries
        noon_start = datetime.combine(d, datetime.min.time()).replace(hour=3)
        noon_end   = datetime.combine(d, datetime.min.time()).replace(hour=12)
        pm3_end    = datetime.combine(d, datetime.min.time()).replace(hour=15)

        noon_df = day_df[(day_df["completeDate"] >= noon_start) & (day_df["completeDate"] < noon_end)]
        pm3_df  = day_df[(day_df["completeDate"] >= noon_start) & (day_df["completeDate"] < pm3_end)]

        result.append({
            "date": date_str,
            "label": label,
            "noon_dd_total": int((noon_df["Task"] == "3dd").sum()),
            "noon_cf_total": int((noon_df["Task"] == "3dcf").sum()),
            "noon_dd_by_emp": _build_by_emp(noon_df, "3dd"),
            "noon_cf_by_emp": _build_by_emp(noon_df, "3dcf"),
            "pm3_dd_total":  int((pm3_df["Task"] == "3dd").sum()),
            "pm3_cf_total":  int((pm3_df["Task"] == "3dcf").sum()),
            "pm3_dd_by_emp": _build_by_emp(pm3_df, "3dd"),
            "pm3_cf_by_emp": _build_by_emp(pm3_df, "3dcf"),
        })

    return result, fetched_at
