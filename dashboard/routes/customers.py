from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse, StreamingResponse
from dashboard.data.cache import cache
import io
import csv

router = APIRouter()

# Columns to display in the table (user-friendly order)
DISPLAY_COLUMNS = [
    'PracticeName', 'FullName', 'AccountManager', 'Type', 'Specialty',
    'City', 'State', 'OfficePhone', 'Email',
    'MTDSales', 'LMSales', 'YTDSales', 'LySales', 'LTDSales',
    'DateOfFirstCase', 'DateOfLastCase',
    'Active', 'Prospect', 'ReferredBy',
]

# Columns available for dropdown filters
FILTER_COLUMNS = ['AccountManager', 'Type', 'Specialty', 'State', 'City']

# Sales columns (formatted as currency)
SALES_COLUMNS = ['MTDSales', 'LMSales', 'YTDSales', 'LySales', 'LTDSales']

# Human-readable column labels
COLUMN_LABELS = {
    'PracticeName': 'Practice Name',
    'FullName': 'Contact',
    'AccountManager': 'Account Manager',
    'Type': 'Type',
    'Specialty': 'Specialty',
    'City': 'City',
    'State': 'State',
    'OfficePhone': 'Phone',
    'Email': 'Email',
    'MTDSales': 'MTD Sales',
    'LMSales': 'Last Month',
    'YTDSales': 'YTD Sales',
    'LySales': 'Last Year',
    'LTDSales': 'Lifetime',
    'DateOfFirstCase': 'First Case',
    'DateOfLastCase': 'Last Case',
    'Active': 'Active',
    'Prospect': 'Prospect',
    'ReferredBy': 'Referred By',
}


def _get_filter_options(df):
    """Get unique values for each filter column."""
    options = {}
    for col in FILTER_COLUMNS:
        if col in df.columns:
            vals = df[col].dropna().unique().tolist()
            options[col] = sorted([str(v) for v in vals if str(v).strip()])
    return options


def _apply_tab_filter(df, tab):
    """Filter DataFrame by tab selection."""
    if tab == 'active' and 'Active' in df.columns and 'Prospect' in df.columns:
        return df[(df['Active'] == 1) & (df['Prospect'] == 0)]
    elif tab == 'prospects' and 'Prospect' in df.columns and 'Active' in df.columns:
        return df[(df['Prospect'] == 1) & (df['Active'] == 0)]
    return df


@router.get("/customers", response_class=HTMLResponse)
async def customers_page(request: Request):
    df = await cache.get("customers")
    metadata = await cache.get_metadata()

    customers = []
    filter_options = {}
    total_count = 0

    if df is not None and not df.empty:
        # Filter out rows with blank/null CustomerID
        df = df[df['CustomerID'].notna() & (df['CustomerID'] != '')]
        total_count = len(df)
        filter_options = _get_filter_options(df)
        # Convert to list of dicts for template
        customers = df.to_dict('records')
        # Convert date objects to strings for JSON serialization
        for row in customers:
            for key in ('DateOfFirstCase', 'DateOfLastCase'):
                if key in row and row[key] is not None:
                    row[key] = str(row[key])

    templates = request.app.state.templates
    return templates.TemplateResponse("pages/customers.html", {
        "request": request,
        "metadata": metadata,
        "active_page": "customers",
        "customers": customers,
        "total_count": total_count,
        "display_columns": DISPLAY_COLUMNS,
        "column_labels": COLUMN_LABELS,
        "filter_columns": FILTER_COLUMNS,
        "filter_options": filter_options,
        "sales_columns": SALES_COLUMNS,
    })


@router.get("/customers/export")
async def customers_export(
    tab: str = Query("all"),
    account_manager: str = Query(None, alias="AccountManager"),
    cust_type: str = Query(None, alias="Type"),
    specialty: str = Query(None, alias="Specialty"),
    state: str = Query(None, alias="State"),
    city: str = Query(None, alias="City"),
):
    df = await cache.get("customers")
    if df is None or df.empty:
        return StreamingResponse(
            io.StringIO("No data available"),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=customers.csv"},
        )

    # Filter out blank CustomerID rows
    df = df[df['CustomerID'].notna() & (df['CustomerID'] != '')]

    # Apply tab filter
    df = _apply_tab_filter(df, tab)

    # Apply column filters
    filters = {
        'AccountManager': account_manager,
        'Type': cust_type,
        'Specialty': specialty,
        'State': state,
        'City': city,
    }
    for col, val in filters.items():
        if val and col in df.columns:
            df = df[df[col].astype(str) == val]

    # Write CSV
    output = io.StringIO()
    writer = csv.writer(output)
    columns = [c for c in DISPLAY_COLUMNS if c in df.columns]
    writer.writerow([COLUMN_LABELS.get(c, c) for c in columns])
    for _, row in df[columns].iterrows():
        writer.writerow([row[c] for c in columns])

    output.seek(0)
    filename = f"customers_{tab}.csv"
    return StreamingResponse(
        output,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )
