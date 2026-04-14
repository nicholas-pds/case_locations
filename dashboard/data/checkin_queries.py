"""Check-In query: Accept Remote Case counts over last 6 business days
(Noon midnight–12pm and All midnight–6pm windows) plus last 30 business
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

    category_trends — list of dicts in canonical workload order:
    [
      {
        "category": "Metal",
        "days": [{"date": "2026-03-15", "count": 8}, ...]  # 30 business days oldest→newest
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

    # Normalize categories to match workload page conventions
    df["Category"] = df["Category"].fillna("").str.strip()
    df.loc[df["Category"] == "", "Category"] = "Other"
    df["Category"] = df["Category"].replace({
        "Airway": "MARPE",
        "Lab to lab": "Lab to Lab",
        "Accessories": "Other",
    })
    df.loc[df["Category"].str.contains("Expander", case=False, na=False), "Category"] = "E\u00b2 Expanders"

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

    # ── 30-business-day category trends ──────────────────────────────────
    # Exclude today — partial day data skews the trendline. End at yesterday.
    # Business days only (no weekends), oldest → newest for chart axis.
    _trend_end = date.today() - timedelta(days=1)
    _trend_days: list = []
    _d = _trend_end
    while len(_trend_days) < 30:
        if _d.weekday() < 5:
            _trend_days.append(_d)
        _d -= timedelta(days=1)
    last_30 = list(reversed(_trend_days))
    last_30_strs = {str(d) for d in last_30}
    trend_df = df[df["date_str"].isin(last_30_strs)].copy()

    all_cats = trend_df["Category"].unique().tolist()

    # Order categories to match workload page canonical order
    CATEGORIES_ORDER = ["Hybrid", "E\u00b2 Expanders", "Lab to Lab", "MARPE",
                        "Metal", "Clear", "Wire Bending", "Other"]
    known = [c for c in CATEGORIES_ORDER if c in all_cats]
    extra = [c for c in all_cats if c not in CATEGORIES_ORDER]
    sorted_cats = known + sorted(extra)

    category_trends = []
    for cat_name in sorted_cats:
        days = []
        for d in last_30:
            day_df = trend_df[trend_df["date_str"] == str(d)]
            count = int((day_df["Category"] == cat_name).sum())
            days.append({"date": str(d), "count": count})
        avg = round(sum(d["count"] for d in days) / len(days)) if days else 0
        category_trends.append({"category": cat_name, "days": days, "avg": avg})

    return records, fetched_at, category_trends
