"""Airway task query: 3dplan and 3dfin-exp counts over last 6 business days."""
import sys
import pandas as pd
from datetime import date, timedelta
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


def fetch_airway_tasks() -> list:
    """
    Execute airway_tasks_plan_export.sql, filter to last 6 business days,
    resolve employee IDs to names, and return structured data for template.

    Returns list of dicts, one per business day (desc: today first):
    {
      "date": "2026-03-30",
      "label": "Mon 3/30",
      "plan_total": 12,
      "fin_total": 8,
      "plan_by_emp": [{"name": "John S.", "count": 3}, ...],
      "fin_by_emp":  [{"name": "Bill K.", "count": 7}, ...],
    }
    """
    df = execute_sql_to_dataframe(str(SQL_DIR / "airway_tasks_plan_export.sql"))
    if df.empty:
        return []

    # Build employee ID → short name map
    emp_df = load_employee_lkups()
    emp_map: dict[int, str] = {}
    if not emp_df.empty:
        for _, row in emp_df.iterrows():
            try:
                emp_map[int(row["Employee ID"])] = _short(str(row["MT Name"]))
            except (ValueError, TypeError):
                pass

    # Normalize column names (strip whitespace)
    df.columns = [c.strip() for c in df.columns]

    # Parse dates
    df["completedate"] = pd.to_datetime(df["completedate"]).dt.date
    df["date_str"] = df["completedate"].astype(str)

    # Filter to last 6 business days
    biz_days = _last_n_business_days(6)
    biz_day_strs = {str(d) for d in biz_days}
    df = df[df["date_str"].isin(biz_day_strs)].copy()

    # Resolve employee ID → short name
    def _resolve(emp_id):
        try:
            return emp_map.get(int(emp_id), f"ID:{emp_id}")
        except (ValueError, TypeError):
            return str(emp_id)

    df["emp_name"] = df["Completed by name"].apply(_resolve)

    result = []
    for d in biz_days:
        date_str = str(d)
        day_df = df[df["date_str"] == date_str]

        # Windows-safe label (avoids %-m which is Linux-only)
        label = d.strftime("%a") + " " + str(d.month) + "/" + str(d.day)

        plan_by = [
            {"name": r["emp_name"], "count": int(r["Sum of 3dplan tasks"])}
            for _, r in day_df.iterrows()
            if int(r["Sum of 3dplan tasks"]) > 0
        ]
        fin_by = [
            {"name": r["emp_name"], "count": int(r["Sum of 3dfin-exp"])}
            for _, r in day_df.iterrows()
            if int(r["Sum of 3dfin-exp"]) > 0
        ]
        plan_by.sort(key=lambda x: x["count"], reverse=True)
        fin_by.sort(key=lambda x: x["count"], reverse=True)

        result.append({
            "date": date_str,
            "label": label,
            "plan_total": int(day_df["Sum of 3dplan tasks"].sum()),
            "fin_total": int(day_df["Sum of 3dfin-exp"].sum()),
            "plan_by_emp": plan_by,
            "fin_by_emp": fin_by,
        })

    return result
