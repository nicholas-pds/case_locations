"""Efficiency report page routes."""
import io
import logging

import pandas as pd
from fastapi import APIRouter, Request, UploadFile, File
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse

from dashboard.data.efficiency_store import (
    load_daily, load_aggregated,
    load_employee_lkups, save_employee_lkups,
    load_teams, save_teams, apply_team_renames, TEAM_RENAME_MAP,
)
from dashboard.data.efficiency_processing import run_full_upload
from dashboard.data.airway_queries import fetch_airway_tasks
from dashboard.data.design_queries import fetch_design_tasks
from dashboard.data.checkin_queries import fetch_checkin_tasks
from dashboard.data.cache import cache

router = APIRouter()
logger = logging.getLogger("dashboard.routes.efficiency")


def _df_to_records(df):
    """Convert DataFrame to JSON-safe list of dicts."""
    if df is None or df.empty:
        return []
    # Convert all values to native Python types
    records = []
    for row in df.to_dict("records"):
        clean = {}
        for k, v in row.items():
            if pd.isna(v):
                v = ""
            elif hasattr(v, "item"):  # numpy scalar
                v = v.item()
            elif hasattr(v, "isoformat"):  # date/datetime
                v = str(v)
            clean[k] = v
        records.append(clean)
    return records


@router.get("/efficiency", response_class=HTMLResponse)
async def efficiency_page(request: Request):
    daily_df = load_daily()
    agg_df = load_aggregated()

    # Sort daily: Date desc, then MT Name asc
    if not daily_df.empty:
        daily_df = daily_df.sort_values(["Date", "MT Name"], ascending=[False, True])

    # Team list driven by teams.csv (not derived from data — canonical config)
    teams = load_teams()

    # Get MM EFF column labels (all efficiency period columns)
    mm_eff_cols = []
    week_eff_cols = []
    week_eff_labels = {}
    if not agg_df.empty:
        all_eff_cols = [c for c in agg_df.columns if c.startswith("Efficiency_")]
        mm_eff_cols = [c for c in all_eff_cols if not c.startswith("Efficiency_Week_")]
        week_eff_cols = [c for c in all_eff_cols if c.startswith("Efficiency_Week_")]
        for col in week_eff_cols:
            n = int(col.replace("Efficiency_Week_", ""))
            if n == 0:
                week_eff_labels[col] = "Curr Week"
            elif n == 1:
                week_eff_labels[col] = "1 Wk Ago"
            else:
                week_eff_labels[col] = f"{n} Wks Ago"

    # Available dates (sorted desc) and latest date for default filter
    available_dates = []
    latest_date = None
    if not daily_df.empty and "Date" in daily_df.columns:
        available_dates = sorted(daily_df["Date"].dropna().unique().tolist(), reverse=True)
        available_dates = [str(d) for d in available_dates]
        latest_date = available_dates[0] if available_dates else None

    templates = request.app.state.templates
    return templates.TemplateResponse("pages/efficiency.html", {
        "request": request,
        "active_page": "efficiency",
        "metadata": await cache.get_metadata(),
        "daily_records": _df_to_records(daily_df),
        "agg_records": _df_to_records(agg_df),
        "teams": teams,
        "mm_eff_cols": mm_eff_cols,
        "week_eff_cols": week_eff_cols,
        "week_eff_labels": week_eff_labels,
        "available_dates": available_dates,
        "latest_date": latest_date,
    })


@router.post("/efficiency/upload")
async def efficiency_upload(file: UploadFile = File(...)):
    """Accept a Gusto CSV upload, run the full pipeline, return JSON result."""
    try:
        contents = await file.read()
        result = run_full_upload(contents, file.filename)
        return JSONResponse(content=result)
    except Exception as e:
        logger.error(f"Upload failed: {e}")
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=400)


@router.get("/efficiency/airway-data")
async def get_airway_data():
    """Lazy-load endpoint for Airway tab data."""
    try:
        records = fetch_airway_tasks()
    except Exception:
        logger.warning("Airway tasks fetch failed", exc_info=True)
        records = []
    return JSONResponse({"records": records})


@router.get("/efficiency/design-data")
async def get_design_data():
    """Lazy-load endpoint for Design tab data."""
    try:
        records, fetched_at = fetch_design_tasks()
    except Exception:
        logger.warning("Design tasks fetch failed", exc_info=True)
        records, fetched_at = [], ""
    return JSONResponse({"records": records, "fetched_at": fetched_at})


@router.get("/efficiency/checkin-data")
async def get_checkin_data():
    """Lazy-load endpoint for Check-In tab data."""
    try:
        records, fetched_at, category_trends = fetch_checkin_tasks()
    except Exception:
        logger.warning("Check-In tasks fetch failed", exc_info=True)
        records, fetched_at, category_trends = [], "", []
    return JSONResponse({"records": records, "fetched_at": fetched_at, "category_trends": category_trends})


@router.get("/efficiency/export/mm")
async def efficiency_export_mm():
    """Export MM EFF aggregated data as CSV."""
    df = load_aggregated()
    if df is None or df.empty:
        return StreamingResponse(
            io.StringIO("No data available"),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=efficiency_mm.csv"},
        )
    output = io.StringIO()
    df.to_csv(output, index=False)
    output.seek(0)
    return StreamingResponse(
        output,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=efficiency_mm.csv"},
    )


# ─────────────────────────────────────────────
# Employee Lookups CRUD
# ─────────────────────────────────────────────

@router.get("/efficiency/employees")
async def get_employees():
    """Return employee lookups as JSON."""
    df = load_employee_lkups()
    return JSONResponse(content=_df_to_records(df))


@router.post("/efficiency/employees")
async def save_employees(request: Request):
    """Save employee lookups from JSON array."""
    try:
        data = await request.json()
        df = pd.DataFrame(data)
        required = ["Employee ID", "MT Name", "Gusto Name", "Team", "Training Plan"]
        for col in required:
            if col not in df.columns:
                return JSONResponse(
                    content={"status": "error", "message": f"Missing column: {col}"},
                    status_code=400,
                )
        save_employee_lkups(df)
        from dashboard.data.efficiency_processing import reprocess_with_employee_lkups
        result = reprocess_with_employee_lkups()
        return JSONResponse(content={"status": "ok", "rows": len(df), **result})
    except Exception as e:
        logger.error(f"Save employees failed: {e}")
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=400)


@router.get("/efficiency/teams")
async def get_teams():
    """Return team list with employee counts."""
    teams = load_teams()
    lkups = load_employee_lkups()
    counts: dict = {}
    if not lkups.empty and "Team" in lkups.columns:
        counts = lkups["Team"].value_counts().to_dict()
    return JSONResponse({"teams": teams, "counts": counts})


@router.post("/efficiency/teams")
async def save_teams_route(request: Request):
    """
    Save updated team list and apply any renames.
    Payload: {"teams": [{"original": str|null, "name": str}, ...]}
    Renames trigger reprocess of daily/parquet data.
    """
    try:
        data = await request.json()
        rows = data.get("teams", [])
        rename_map = {
            r["original"]: r["name"]
            for r in rows
            if r.get("original") and r["original"] != r["name"]
        }
        new_names = [r["name"] for r in rows if r.get("name", "").strip()]
        save_teams(new_names)
        result: dict = {"status": "ok", "teams": len(new_names)}
        if rename_map:
            from dashboard.data.efficiency_processing import reprocess_with_employee_lkups
            result["migration"] = apply_team_renames(rename_map)
            result["reprocess"] = reprocess_with_employee_lkups()
        return JSONResponse(result)
    except Exception as e:
        logger.error(f"Save teams failed: {e}")
        return JSONResponse({"status": "error", "message": str(e)}, status_code=400)


@router.post("/efficiency/migrate-teams")
async def migrate_teams_once():
    """One-time migration: apply TEAM_RENAME_MAP to all historical data."""
    try:
        from dashboard.data.efficiency_processing import reprocess_with_employee_lkups
        stats = apply_team_renames(TEAM_RENAME_MAP)
        result = reprocess_with_employee_lkups()
        return JSONResponse({"status": "ok", "migration": stats, "reprocess": result})
    except Exception as e:
        logger.error(f"migrate_teams_once failed: {e}")
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)
