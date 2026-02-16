import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Paths
PROJECT_ROOT = Path(__file__).parent.parent
SQL_DIR = PROJECT_ROOT / "sql_query"
TEMPLATE_DIR = Path(__file__).parent / "templates"
STATIC_DIR = Path(__file__).parent / "static"

# Server
DASHBOARD_PORT = int(os.getenv("DASHBOARD_PORT", "8050"))
DASHBOARD_HOST = os.getenv("DASHBOARD_HOST", "0.0.0.0")

# Auth
DASHBOARD_PASSWORD = os.getenv("DASHBOARD_PASSWORD", "Partners1724!")
SECRET_KEY = os.getenv("DASHBOARD_SECRET_KEY", "partners-dashboard-secret-key-change-me")
SESSION_MAX_AGE = 12 * 60 * 60  # 12 hours in seconds

# Refresh
REFRESH_INTERVAL_SECONDS = 60
BUSINESS_HOURS_START = 6   # 6 AM
BUSINESS_HOURS_END = 18    # 6 PM

# Query lookback
DAYS_LOOKBACK = 6
WORKLOAD_DAYS_RANGE = 9

# MARPE planning locations to exclude from main view
MARPE_EXCLUDED_LOCATIONS = [
    'New Cases',
    'New Cases How to Proceed',
    'New Cases Waiting For Scans',
    'Email Plan Case',
    'Email Follow Up',
    'Zoom Set Up',
    'Zoom Consult',
    'Zoom Export Needed',
    'Zoom Waiting Approval',
    'Airway Zoom Plan',
    'Airway Email Approval',
    'Airway Planning',
    'Airway Email Plan',
    'Airway Zoom Approval',
]

# Map raw DB location names to display names
LOCATION_ALIASES = {
    'Marpe Finish': 'Marpe',
    'Marpe Assembly': 'Marpe',
    'Marpe Weld': 'Marpe',
    'Marpe PreWeld': 'Marpe',
    'Marpe Dlyte': 'Marpe',
    'Wire E&P': 'Wire Finishing/Polishing',
    'Shipping Hold Tray': 'Shipping Hold Table',
    'Intake Hold Tray': 'Intake Hold Table',
}

# Location display order for the main page grid
LOCATION_DISPLAY_ORDER = [
    'Design Cart', '3D Design', '3D Manufacturing', 'Oven',
    'Metal Shelf', 'Metal Finish', 'Tumbler',
    'Banding', 'Metal Bending', 'Welding', 'Metal Polishing',
    'Marpe', 'Wire Shelf', 'Wire Bending',
    'Acrylic', 'Wire Finishing/Polishing', 'Essix Shelf',
    'QC', 'Production Floor Desk', 'Shipping Hold Table', 'Clinical Director', 'Intake Hold Table',
]

# Category colors for CSS classes
CATEGORY_COLORS = {
    'Metal': '#607D8B',
    'Clear': '#2196F3',
    'Wire Bending': '#FF9800',
    'MARPE': '#9C27B0',
    'Hybrid': '#009688',
    'E2 Expanders': '#F44336',
    'Airway': '#E91E63',
    'Lab to Lab': '#7B1FA2',
    'Other': '#B0BEC5',
}

# Airway workflow stage grouping
AIRWAY_STAGE_GROUPS = {
    'NEW CASES': ['New Cases', 'New Cases How to Proceed', 'New Cases Waiting For Scans'],
    'EMAIL': ['Email Plan Case', 'Email Follow Up'],
    'ZOOM': ['Zoom Set Up', 'Zoom Consult', 'Zoom Export Needed', 'Zoom Waiting Approval'],
}
