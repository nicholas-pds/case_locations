"""Efficiency report page routes."""
import io
import logging

import pandas as pd
from fastapi import APIRouter, Request, UploadFile, File
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse

from dashboard.data.efficiency_store import (
    load_daily, load_aggregated, load_midday, save_midday,
    load_tech_constants, save_tech_constants,
)
from dashboard.data.efficiency_processing import run_full_upload, process_midday_snapshot
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
            if hasattr(v, "item"):  # numpy scalar
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
    noon_df = load_midday("noon")
    pm3_df = load_midday("3pm")

    # Sort daily: Date desc, then MT Name asc
    if not daily_df.empty:
        daily_df = daily_df.sort_values(["Date", "MT Name"], ascending=[False, True])

    # Get unique teams for filter dropdown (from daily data, exclude z_Not On Report)
    teams = []
    if not daily_df.empty and "Team" in daily_df.columns:
        teams = sorted([
            t for t in daily_df["Team"].dropna().unique().tolist()
            if t and t != "z_Not On Report"
        ])

    # Get MM EFF column labels (all efficiency period columns)
    mm_eff_cols = []
    if not agg_df.empty:
        mm_eff_cols = [c for c in agg_df.columns if c.startswith("Efficiency_")]

    # Available dates (sorted desc) and latest date for default filter
    available_dates = []
    latest_date = None
    if not daily_df.empty and "Date" in daily_df.columns:
        available_dates = sorted(daily_df["Date"].dropna().unique().tolist(), reverse=True)
        available_dates = [str(d) for d in available_dates]
        latest_date = available_dates[0] if available_dates else None

    # Load tech constants and join goals onto noon/3pm data
    constants_df = load_tech_constants()
    constants_records = _df_to_records(constants_df)

    # Join constants onto noon/3pm by Name (left join preserves all midday rows)
    if not constants_df.empty:
        # Only Morning-shift constants for goal columns (Evening/Outsource have different semantics)
        morning_constants = constants_df[constants_df["ShiftType"] == "Morning"][["Name", "Noon", "3PM"]].copy()
        morning_constants = morning_constants.rename(columns={"Noon": "Noon_Goal", "3PM": "PM3_Goal"})
        if not noon_df.empty:
            noon_df = noon_df.merge(morning_constants, on="Name", how="left")
        if not pm3_df.empty:
            pm3_df = pm3_df.merge(morning_constants, on="Name", how="left")

    templates = request.app.state.templates
    return templates.TemplateResponse("pages/efficiency.html", {
        "request": request,
        "active_page": "efficiency",
        "metadata": await cache.get_metadata(),
        "daily_records": _df_to_records(daily_df),
        "agg_records": _df_to_records(agg_df),
        "noon_records": _df_to_records(noon_df),
        "pm3_records": _df_to_records(pm3_df),
        "constants_records": constants_records,
        "teams": teams,
        "mm_eff_cols": mm_eff_cols,
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


@router.post("/efficiency/refresh-midday")
async def efficiency_refresh_midday():
    """Manually trigger noon and 3pm midday snapshot refresh."""
    try:
        results = {}
        for window in ("noon", "3pm"):
            df = process_midday_snapshot(window)
            if not df.empty:
                save_midday(window, df)
                results[window] = len(df)
            else:
                results[window] = 0
        return JSONResponse(content={"status": "ok", "rows": results})
    except Exception as e:
        logger.error(f"Midday refresh failed: {e}")
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=400)


@router.get("/efficiency/constants")
async def get_constants():
    """Return tech constants as JSON."""
    df = load_tech_constants()
    return JSONResponse(content=_df_to_records(df))


@router.post("/efficiency/constants")
async def save_constants(request: Request):
    """Save tech constants from JSON array."""
    try:
        data = await request.json()
        df = pd.DataFrame(data)
        # Ensure expected columns exist
        for col in ["Name", "Noon", "3PM", "ShiftType", "DesignType"]:
            if col not in df.columns:
                return JSONResponse(
                    content={"status": "error", "message": f"Missing column: {col}"},
                    status_code=400,
                )
        save_tech_constants(df)
        return JSONResponse(content={"status": "ok", "rows": len(df)})
    except Exception as e:
        logger.error(f"Save constants failed: {e}")
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=400)


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
