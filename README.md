# Partners Dental Lab — Internal Analytics Dashboard

Internal web dashboard for tracking lab cases, efficiency, remakes, and daily operations. Self-hosted on a Windows VM and accessible at [https://partnersds.live](https://partnersds.live).

---

## System Architecture

| Layer | What it is |
|---|---|
| MagicTouch DB | Source of truth — live lab data (cases, orders, tasks, etc.) |
| Nightly sync (2 AM) | Automated job copies new records to cloud |
| Cloud DB (mirror) | Independent PostgreSQL database on Render — no MT dependency going forward |
| Dashboard / Webapp | Built on cloud DB + MT. Self-hosted on VM. Cloudflare tunnel connector handles DNS, SSL, and networking. Domain: partnersds.live |

---

## Tech Stack

- **Python 3.13+**, FastAPI, Uvicorn
- **Jinja2** templates, **Alpine.js**, **htmx**, **Chart.js 4.4.7**
- **pyodbc** (SQL Server / MagicTouch), **psycopg2** (PostgreSQL / Render)
- **Pandas** + **PyArrow** (parquet caching)
- Server-sent events for live refresh

---

## Dashboard Pages

| Route | Page | Description |
|---|---|---|
| `/` | Case Locations | Location grid with case counts + filterable data table |
| `/workload` | Workload | Stacked bar chart (Invoiced vs In Production) + category breakdown |
| `/airway` | Airway Workflow | Workflow stage cards — New Cases, Email, Zoom sections |
| `/airway-hold` | Airway Hold Status | Hold status table with follow-up dates |
| `/local-delivery` | Local Delivery | Cases flagged for local delivery |
| `/overdue-noscan` | Overdue No Scan | Overdue cases not scanned within 4 hours |
| `/daily-summary` | Daily Summary | End-of-day case summary view |
| `/customers` | Customers | Customer-level case and order data |
| `/efficiency` | Efficiency | Technician efficiency metrics from parquet cache |
| `/remakes` | Remakes | Remake tracking with AM tabs, charts, notes, and meeting view |

---

## Setup & Running

### Prerequisites

- Python 3.13+
- [uv](https://docs.astral.sh/uv/) package manager
- Microsoft ODBC Driver 17 (or 18) for SQL Server

### Install

```bash
git clone <repo-url>
cd case_locations
uv sync
```

### Configure

```bash
cp .env.example .env
# Fill in credentials (see Configuration section below)
```

### Run

```bash
uv run python -m dashboard.run
# or
python dashboard/run.py
```

Default URL: [http://localhost:8050](http://localhost:8050)

---

## Configuration

Key `.env` variables:

| Variable | Description |
|---|---|
| `DASHBOARD_PASSWORD` | Password for the login page |
| `DASHBOARD_PORT` | Port the server listens on (default: `8050`) |
| `DASHBOARD_SECRET_KEY` | Secret key for session cookies |
| `SQL_SERVER` | MagicTouch SQL Server hostname or IP |
| `SQL_DATABASE` | Database name |
| `SQL_USERNAME` | SQL Server login |
| `SQL_PASSWORD` | SQL Server password |
| `PG_HOST` | PostgreSQL (Render) host |
| `PG_DATABASE` | PostgreSQL database name |
| `PG_USER` | PostgreSQL user |
| `PG_PASSWORD` | PostgreSQL password |

---

## User Inputs (CSVs)

Files in `User_Inputs/` are read at runtime and control dashboard behavior:

| File | Purpose |
|---|---|
| `tech_constants.csv` | Per-technician constants used in efficiency calculations |
| `employee_lkups.csv` | Employee ID to name mapping (used in Remakes module) |
| `revenue_goals.csv` | Revenue targets for chart overlays |
| `remake_notes.csv` | Persistent notes per case, upserted by MainCaseNumber |

---

## Background Sync

- **`sync/`** — PostgreSQL sync engine. Runs nightly at 2 AM. Copies new records from MagicTouch SQL Server to cloud DB.
- **`dashboard/data/refresh.py`** — In-app cache refresh loop. Runs every 60 seconds during business hours (6 AM–6 PM). Queries both DBs and populates in-memory cache.

---

## Deployment

The dashboard runs as a Windows service via [NSSM](https://nssm.cc) and starts automatically on boot.

| Service | What it runs |
|---|---|
| `PartnersDashboard` | FastAPI web app on port 8050 |
| `CloudflaredTunnel` | Cloudflare tunnel → `partnersds.live` |

Cloudflare handles DNS, SSL, and networking — no VPN or firewall port-forwarding required.

Public URL: **https://partnersds.live**

**Manage services (Admin PowerShell):**

```powershell
Get-Service PartnersDashboard, CloudflaredTunnel
Start-Service PartnersDashboard
Restart-Service CloudflaredTunnel
```

NSSM installed at `C:\nssm\nssm.exe`. Edit a service:

```powershell
C:\nssm\nssm.exe edit PartnersDashboard
```

---

## Roadmap

| Horizon | Initiative | Notes |
|---|---|---|
| Now | UI improvements | Cleaner layouts, better readability, consistent theme |
| Now | External Remakes | Drill-down views, STL file viewer, new categories, learning opportunity breakouts |
| Near-term | Efficiency — Current | Needs tie-out + PDF formatting |
| Mid-term | Check-in team efficiency | Add to existing efficiency view |
| Mid-term | Internal Rejects | Copy from existing dashboard |
| Mid-term | Write-access to DB | Two-way data syncing |
| Mid-term | Financials | Sales, expenses, revenue, department breakouts |
| Long-term | Lab TVs — Live Dashboards | Needs hardware budget discussion. Technician case queue. |
| Long-term | Case Task Tracking | Handle and record all case task activity |
| Long-term | Full custom lab management software | Flip a switch to drop MT entirely |
