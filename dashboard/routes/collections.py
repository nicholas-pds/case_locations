"""Past Due Collections page routes."""
import logging

import pandas as pd
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

from dashboard.data.cache import cache
from dashboard.data.collections_queries import (
    get_cached_collections,
    refresh_collections_cache,
    load_collections_log,
    save_collection_entry,
    save_collection_completed,
    _collections_lock,
)

router = APIRouter()
logger = logging.getLogger("dashboard.routes.collections")


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
            elif hasattr(v, "item"):
                v = v.item()
            elif hasattr(v, "isoformat"):
                v = str(v)
            clean[k] = v
        records.append(clean)
    return records


def _build_sections(accounts_df: pd.DataFrame, cases_df: pd.DataFrame):
    """Join accounts + cases, split into 3 sections, compute stats."""
    if accounts_df is None:
        accounts_df = pd.DataFrame()
    if cases_df is None:
        cases_df = pd.DataFrame()

    df = accounts_df.copy() if not accounts_df.empty else pd.DataFrame()
    cases_by_customer: dict = {}

    if not df.empty:
        df["CustomerID"] = df["CustomerID"].astype(str)

        # Cases grouping
        if not cases_df.empty:
            cases_df = cases_df.copy()
            cases_df["CustomerID"] = cases_df["CustomerID"].astype(str)
            counts = cases_df.groupby("CustomerID").size().to_dict()
            for cid, grp in cases_df.groupby("CustomerID"):
                cases_by_customer[str(cid)] = _df_to_records(grp)
        else:
            counts = {}

        df["OpenCaseCount"] = df["CustomerID"].map(counts).fillna(0).astype(int)
        df["TotalPastDue"] = (df["PastDue90"].astype(float)
                              + df["PastDueOver90"].astype(float))

        is_smile = df["DentalGroup"].fillna("") == "Smile Doctors"
        section1 = df[~is_smile & (df["PastDue90"] >= 500)]
        section2 = df[~is_smile & (df["PastDue90"] < 500)]
        section3 = df[is_smile]
    else:
        section1 = section2 = section3 = pd.DataFrame()

    def _stats(s: pd.DataFrame) -> dict:
        if s.empty:
            return {"count": 0, "past_due_90": 0.0, "past_due_over_90": 0.0}
        return {
            "count": int(len(s)),
            "past_due_90": float(s["PastDue90"].sum()),
            "past_due_over_90": float(s["PastDueOver90"].sum()),
        }

    stats = {
        "section1": _stats(section1),
        "section2": _stats(section2),
        "section3": _stats(section3),
    }
    overall = {
        "count": stats["section1"]["count"] + stats["section2"]["count"] + stats["section3"]["count"],
        "past_due_90": stats["section1"]["past_due_90"] + stats["section2"]["past_due_90"] + stats["section3"]["past_due_90"],
        "past_due_over_90": stats["section1"]["past_due_over_90"] + stats["section2"]["past_due_over_90"] + stats["section3"]["past_due_over_90"],
    }

    return (
        _df_to_records(section1),
        _df_to_records(section2),
        _df_to_records(section3),
        stats,
        overall,
        cases_by_customer,
    )


def _build_log_dict(log_df: pd.DataFrame) -> dict:
    if log_df is None or log_df.empty:
        return {}
    out = {}
    for row in _df_to_records(log_df):
        cid = row.get("CustomerID")
        if not cid:
            continue
        out[str(cid)] = {
            "Outcome": row.get("Outcome", ""),
            "Notes": row.get("Notes", ""),
            "WhoLogged": row.get("WhoLogged", ""),
            "LastContacted": row.get("LastContacted", ""),
            "Completed": row.get("Completed", "0") or "0",
        }
    return out


@router.get("/collections", response_class=HTMLResponse)
async def collections_page(request: Request):
    cached = get_cached_collections()
    accounts_df = cached["accounts"]
    cases_df = cached["cases"]

    s1, s2, s3, section_stats, overall_stats, cases_by_customer = _build_sections(
        accounts_df, cases_df
    )

    log_entries = _build_log_dict(load_collections_log())

    templates = request.app.state.templates
    return templates.TemplateResponse("pages/collections.html", {
        "request": request,
        "active_page": "collections",
        "metadata": await cache.get_metadata(),
        "last_refresh": cached["last_refresh"],
        "section1_records": s1,
        "section2_records": s2,
        "section3_records": s3,
        "section_stats": section_stats,
        "overall_stats": overall_stats,
        "cases_by_customer": cases_by_customer,
        "log_entries": log_entries,
    })


@router.post("/collections/refresh")
async def collections_refresh():
    try:
        result = await refresh_collections_cache()
        return JSONResponse({"status": "ok", **result})
    except Exception as e:
        logger.error(f"Collections refresh failed: {e}")
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


@router.post("/collections/log")
async def collections_log(request: Request):
    try:
        body = await request.json()
        customer_id = str(body.get("customer_id", "")).strip()
        if not customer_id:
            return JSONResponse(
                {"status": "error", "message": "customer_id required"},
                status_code=400,
            )
        outcome = body.get("outcome")
        notes = body.get("notes")
        who_logged = body.get("who_logged")
        mark_contacted = bool(body.get("mark_contacted", False))

        async with _collections_lock:
            last_contacted = save_collection_entry(
                customer_id,
                outcome=outcome,
                notes=notes,
                who_logged=who_logged,
                mark_contacted=mark_contacted,
            )
        return JSONResponse({
            "status": "ok",
            "last_contacted": last_contacted,
        })
    except Exception as e:
        logger.error(f"Collections log save failed: {e}")
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


@router.post("/collections/completed")
async def collections_completed(request: Request):
    try:
        body = await request.json()
        customer_id = str(body.get("customer_id", "")).strip()
        if not customer_id:
            return JSONResponse(
                {"status": "error", "message": "customer_id required"},
                status_code=400,
            )
        completed = bool(body.get("completed", False))
        async with _collections_lock:
            save_collection_completed(customer_id, completed)
        return JSONResponse({"status": "ok"})
    except Exception as e:
        logger.error(f"Collections completed save failed: {e}")
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)
