import csv
import io
import json
from datetime import date
from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from dashboard.data.cache import cache

router = APIRouter()

HOLD_STATUS_ORDER = [
    'Production, Waiting on Scan',
    'Airway, How to Proceed',
    'Email Plan,Waiting on Approval',
    'Zoom Plan, Waiting on Approval',
]


@router.get("/airway-hold", response_class=HTMLResponse)
async def airway_hold_page(request: Request):
    df = await cache.get("airway_hold_status")
    metadata = await cache.get_metadata()

    cases = df.to_dict('records') if df is not None and not df.empty else []

    # Get unique values for filters
    hold_statuses = []
    status_counts = {}
    if df is not None and not df.empty and 'HoldStatus' in df.columns:
        hold_statuses = sorted(df['HoldStatus'].dropna().unique().tolist())
        status_counts = df.groupby('HoldStatus').size().to_dict()

    status_counts_json = json.dumps(status_counts)
    cases_json = json.dumps(cases, default=str)

    templates = request.app.state.templates
    return templates.TemplateResponse("pages/airway_hold.html", {
        "request": request,
        "cases": cases,
        "cases_json": cases_json,
        "hold_statuses": hold_statuses,
        "status_counts": status_counts,
        "status_counts_json": status_counts_json,
        "hold_status_order": HOLD_STATUS_ORDER,
        "metadata": metadata,
        "active_page": "airway-hold",
    })


@router.get("/airway-hold/data")
async def airway_hold_data():
    from fastapi.responses import JSONResponse
    df = await cache.get("airway_hold_status")
    if df is not None and not df.empty and 'HoldStatus' in df.columns:
        counts = df.groupby('HoldStatus').size().to_dict()
        rows = df.to_dict('records')
    else:
        counts = {}
        rows = []
    return JSONResponse(content=json.loads(json.dumps({"rows": rows, "counts": counts}, default=str)))


@router.get("/airway-hold/export")
async def airway_hold_export(hold_status: list[str] = Query(default=[])):
    df = await cache.get("airway_hold_status")
    if df is None or df.empty:
        records = []
    else:
        filtered = df.copy()
        if hold_status:
            filtered = filtered[filtered["HoldStatus"].isin(hold_status)]
        records = filtered.to_dict("records")

    columns = ["CaseNumber", "PanNumber", "DoctorName", "PracticeName", "PatientName",
                "CreateDate", "ShipDate", "HoldDate", "HoldStatus", "HoldReason",
                "FollowUpType", "FollowUpDate"]

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=columns, extrasaction="ignore", lineterminator="\n")
    writer.writeheader()
    for row in records:
        writer.writerow({col: (row.get(col, "") or "") for col in columns})

    filename = f"airway_hold_{date.today().isoformat()}.csv"
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )
