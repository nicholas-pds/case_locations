"""Remakes dashboard page routes."""
import asyncio
import io
import logging
import os
from pathlib import Path

import pandas as pd
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, Response

from dashboard.data.remakes_queries import (
    get_cached_remakes,
    refresh_remakes_cache,
    get_db_connection,
    load_remake_notes,
    save_remake_note,
    get_current_week_bounds,
    get_tasks_for_case,
    get_notes_for_case,
    _apply_employee_names,
)
from dashboard.data.cache import cache
from dashboard.config import DOCS_SERVER_USER, DOCS_SERVER_PASS

router = APIRouter()
_DOCS_BASE = r"\\APP-SERVER\DLCPMImages\CaseDocuments"
_DOCS_SHARE = r"\\APP-SERVER\DLCPMImages"
logger = logging.getLogger("dashboard.routes.remakes")


def _mount_docs_share() -> None:
    """Mount the UNC docs share with stored credentials (Windows net use)."""
    if not (DOCS_SERVER_USER and DOCS_SERVER_PASS):
        logger.warning("DOCS_SERVER_USER/PASS not set — UNC share access may fail")
        return
    import subprocess
    result = subprocess.run(
        ["net", "use", _DOCS_SHARE, DOCS_SERVER_PASS, f"/user:{DOCS_SERVER_USER}", "/persistent:no"],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        logger.warning(f"net use failed (may already be connected): {result.stderr.strip()}")
    else:
        logger.info(f"Mounted docs share: {_DOCS_SHARE} as {DOCS_SERVER_USER}")

_mount_docs_share()


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
        "revenue_records": _df_to_records(cached["revenue"]),
        "saved_notes": saved_notes,
        "week_start": str(week_start),
        "week_end": str(week_end),
    })


@router.post("/remakes/refresh")
async def remakes_refresh():
    try:
        result = await refresh_remakes_cache()
        return JSONResponse({"status": "ok", **result})
    except Exception as e:
        logger.error(f"Remakes refresh failed: {e}")
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


@router.get("/remakes/all-details")
async def remakes_all_details():
    cached = get_cached_remakes()
    return JSONResponse({
        "tasks": _df_to_records(cached.get("tasks")),
        "notes": _df_to_records(cached.get("notes_text")),
        "documents": _df_to_records(cached.get("documents")),
    })


@router.get("/remakes/attachment")
async def get_attachment(path: str, thumb: int = 0):
    # External URLs stored in FilePath — cannot serve locally
    if path.startswith("http://") or path.startswith("https://"):
        raise HTTPException(400, "External URL — not served here")
    if ".." in path:
        raise HTTPException(400, "Invalid path")
    try:
        # path is relative to \\APP-SERVER\DLCPMImages (e.g. "CaseDocuments/subdir/file.jpg")
        parts = [p for p in path.replace("\\", "/").split("/") if p]
        full_path = Path(_DOCS_SHARE).joinpath(*parts)
        # security: must stay within the share
        try:
            full_path.relative_to(Path(_DOCS_SHARE))
        except ValueError:
            raise HTTPException(403, "Access denied")

        if not full_path.exists():
            raise HTTPException(404, "File not found")

        suffix = full_path.suffix.lower()
        headers = {"Cache-Control": "max-age=3600"}
        loop = asyncio.get_running_loop()

        if suffix in (".jpg", ".jpeg", ".png", ".gif", ".webp"):
            if thumb:
                from PIL import Image
                def _make_thumb():
                    with Image.open(full_path) as img:
                        img.thumbnail((120, 120))
                        buf = io.BytesIO()
                        img.save(buf, format="JPEG", quality=80)
                        return buf.getvalue()
                data = await loop.run_in_executor(None, _make_thumb)
            else:
                data = await loop.run_in_executor(None, full_path.read_bytes)
            return Response(data, media_type="image/jpeg", headers=headers)

        elif suffix == ".pdf":
            data = await loop.run_in_executor(None, full_path.read_bytes)
            return Response(data, media_type="application/pdf", headers=headers)

        else:
            data = await loop.run_in_executor(None, full_path.read_bytes)
            return Response(
                data,
                media_type="application/octet-stream",
                headers={**headers, "Content-Disposition": f"attachment; filename={full_path.name}"},
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Attachment error for path '{path}': {e}", exc_info=True)
        raise HTTPException(500, "Error serving file")


@router.get("/remakes/case-details")
async def remakes_case_details(main_id: int, og_id: int):
    conn = get_db_connection()
    try:
        tasks_df = get_tasks_for_case(conn, main_id, og_id)
        notes_df = get_notes_for_case(conn, main_id, og_id)
        tasks_df = _apply_employee_names(tasks_df, "CompletedBy", "CompletedByName")
        notes_df = _apply_employee_names(notes_df, "UserID", "UserName")
        return JSONResponse({
            "tasks": _df_to_records(tasks_df),
            "notes": _df_to_records(notes_df),
        })
    except Exception as e:
        logger.error(f"Case details failed: {e}")
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
