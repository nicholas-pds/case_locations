import csv
import io
from datetime import date
from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from dashboard.data.cache import cache

router = APIRouter()


@router.get("/airway-hold", response_class=HTMLResponse)
async def airway_hold_page(request: Request):
    df = await cache.get("airway_hold_status")
    metadata = await cache.get_metadata()

    cases = df.to_dict('records') if df is not None and not df.empty else []

    # Get unique values for filters
    hold_statuses = []
    if df is not None and not df.empty and 'HoldStatus' in df.columns:
        hold_statuses = sorted(df['HoldStatus'].dropna().unique().tolist())

    templates = request.app.state.templates
    return templates.TemplateResponse("pages/airway_hold.html", {
        "request": request,
        "cases": cases,
        "hold_statuses": hold_statuses,
        "metadata": metadata,
        "active_page": "airway-hold",
    })


@router.get("/airway-hold/export")
async def airway_hold_export(hold_status: str = Query(default="")):
    df = await cache.get("airway_hold_status")
    if df is None or df.empty:
        records = []
    else:
        filtered = df.copy()
        if hold_status:
            filtered = filtered[filtered["HoldStatus"] == hold_status]
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
