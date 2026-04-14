"""Check-In query: Accept Remote Case counts over last 6 business days
(Noon midnight–12pm and All midnight–6pm windows) plus last 30 calendar
days grouped by product category for trend charts."""
import sys
import pandas as pd
from datetime import date, datetime, time, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))
SQL_DIR = PROJECT_ROOT / "sql_query"

from src.db_handler import execute_sql_to_dataframe


def _last_n_business_days(n: int) -> list:
    """Return last n Mon-Fri dates (including today if weekday), desc order."""
    result, d = [], date.today()
    while len(result) < n:
        if d.weekday() < 5:  # 0=Mon, 4=Fri
            result.append(d)
        d -= timedelta(days=1)
    return result


def _short(name: str) -> str:
    """Convert 'John Smith' → 'John S.' (matches JS shortName logic)."""
    parts = name.strip().split()
    if len(parts) < 2:
        return name
    return parts[0] + " " + parts[-1][0] + "."


def fetch_checkin_tasks() -> tuple:
    """
    Execute checkin_export.sql, build Noon/All window records for the last
    6 business days and 30-day category trends.

    Returns (records, fetched_at, category_trends) where:

    records — list of dicts, one per business day (desc: today first):
    {
      "date": "2026-04-14",
      "label": "Tue 4/14",
      "noon_total": 10,
      "noon_by_emp": [{"name": "John S.", "count": 3}, ...],
      "all_total": 26,
      "all_by_emp": [{"name": "John S.", "count": 5}, ...],
    }

    category_trends — list of dicts sorted by 30-day total desc:
    [
      {
        "category": "Metal",
        "days": [{"date": "2026-03-15", "count": 8}, ...]  # 30 entries oldest→newest
      },
      ...
    ]

    fetched_at — "2:34 PM" string
    """
    fetched_at = datetime.now().strftime("%I:%M %p").lstrip("0")

    df = execute_sql_to_dataframe(str(SQL_DIR / "checkin_export.sql"))
    if df.empty:
        return [], fetched_at, []

    # Normalize column names
    df.columns = [c.strip() for c in df.columns]

    # Parse CreateDate as full datetime for time-window filtering
    df["CreateDate"] = pd.to_datetime(df["CreateDate"])
    df["date_only"] = df["CreateDate"].dt.date
    df["date_str"] = df["date_only"].astype(str)

    # Short-format employee name (CreatedBy is a display name in CaseAuditTrail)
    df["emp_name"] = df["UserName"].apply(
        lambda n: _short(str(n)) if pd.notna(n) and str(n).strip() else "Unknown"
    )

    df["Category"] = df["Category"].fillna("Other")

    # ── 6-day Noon / All records ──────────────────────────────────────────
    biz_days = _last_n_business_days(6)
    biz_day_strs = {str(d) for d in biz_days}
    biz_df = df[df["date_str"].isin(biz_day_strs)].copy()

    def _by_emp(window_df: pd.DataFrame) -> list:
        if window_df.empty:
            return []
        grp = window_df.groupby("emp_name").size().reset_index(name="count")
        rows = [{"name": r["emp_name"], "count": int(r["count"])} for _, r in grp.iterrows()]
        rows.sort(key=lambda x: x["count"], reverse=True)
        return rows

    records = []
    for d in biz_days:
        date_str = str(d)
        day_df = biz_df[biz_df["date_str"] == date_str].copy()
        label = d.strftime("%a") + " " + str(d.month) + "/" + str(d.day)

        midnight = datetime.combine(d, time(0, 0))
        noon_end = datetime.combine(d, time(12, 0))
        all_end  = datetime.combine(d, time(18, 0))

        noon_df = day_df[(day_df["CreateDate"] >= midnight) & (day_df["CreateDate"] < noon_end)]
        all_df  = day_df[(day_df["CreateDate"] >= midnight) & (day_df["CreateDate"] < all_end)]

        records.append({
            "date": date_str,
            "label": label,
            "noon_total": len(noon_df),
            "noon_by_emp": _by_emp(noon_df),
            "all_total": len(all_df),
            "all_by_emp": _by_emp(all_df),
        })

    # ── 30-day category trends ────────────────────────────────────────────
    today = date.today()
    last_30 = [today - timedelta(days=i) for i in range(29, -1, -1)]  # oldest → newest
    last_30_strs = {str(d) for d in last_30}
    trend_df = df[df["date_str"].isin(last_30_strs)].copy()

    all_cats = trend_df["Category"].unique().tolist()

    # Sort categories by 30-day total desc (most active first)
    cat_totals = {cat: int((trend_df["Category"] == cat).sum()) for cat in all_cats}
    sorted_cats = sorted(all_cats, key=lambda c: cat_totals[c], reverse=True)

    category_trends = []
    for cat_name in sorted_cats:
        days = []
        for d in last_30:
            day_df = trend_df[trend_df["date_str"] == str(d)]
            count = int((day_df["Category"] == cat_name).sum())
            days.append({"date": str(d), "count": count})
        category_trends.append({"category": cat_name, "days": days})

    return records, fetched_at, category_trends
