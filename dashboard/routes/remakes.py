"""Remakes dashboard page routes."""
import logging

import pandas as pd
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

from dashboard.data.remakes_queries import (
    get_cached_remakes,
    refresh_remakes_cache,
    get_db_connection,
    load_remake_notes,
    save_remake_note,
    get_current_week_bounds,
)
from dashboard.data.cache import cache

router = APIRouter()
logger = logging.getLogger("dashboard.routes.remakes")


def _df_to_records(df):
    """Convert DataFrame to JSON-safe list of dicts."""
    if df is None or df.empty:
        return []
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


@router.get("/remakes", response_class=HTMLResponse)
async def remakes_page(request: Request):
    cached = get_cached_remakes()

    all_df = cached["all"]
    if all_df is None:
        all_df = pd.DataFrame()

    # Derive meeting view: exclude Full Charge cases
    if not all_df.empty and "Remake" in all_df.columns:
        meeting_df = all_df[all_df["Remake"] != "Remake Full Charge"].copy()
    else:
        meeting_df = all_df.copy()

    # Load persisted notes
    saved_notes_df = load_remake_notes()
    saved_notes = {
        row["MainCaseNumber"]: row["Note"]
        for row in _df_to_records(saved_notes_df)
        if row.get("MainCaseNumber")
    }

    week_start, week_end = get_current_week_bounds()

    templates = request.app.state.templates
    return templates.TemplateResponse("pages/remakes.html", {
        "request": request,
        "active_page": "remakes",
        "metadata": await cache.get_metadata(),
        "last_refresh": cached["last_refresh"],
        "meeting_records": _df_to_records(meeting_df),
        "all_records": _df_to_records(all_df),
        "task_records": _df_to_records(cached["tasks"]),
        "call_note_records": _df_to_records(cached["notes_text"]),
        "revenue_records": _df_to_records(cached["revenue"]),
        "saved_notes": saved_notes,
        "week_start": str(week_start),
        "week_end": str(week_end),
    })


@router.post("/remakes/refresh")
async def remakes_refresh():
    conn = get_db_connection()
    try:
        result = await refresh_remakes_cache(conn)
        return JSONResponse({"status": "ok", **result})
    except Exception as e:
        logger.error(f"Remakes refresh failed: {e}")
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)
    finally:
        conn.close()


@router.post("/remakes/notes")
async def save_note(request: Request):
    try:
        body = await request.json()
        case_number = body.get("case_number", "")
        note_text = body.get("note", "")
        if not case_number:
            return JSONResponse(
                {"status": "error", "message": "case_number required"},
                status_code=400,
            )
        save_remake_note(case_number, note_text)
        return JSONResponse({"status": "ok"})
    except Exception as e:
        logger.error(f"Save note failed: {e}")
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)
