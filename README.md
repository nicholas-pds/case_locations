## 🚀 Case Locations Report Generator (uv Edition)

A small Python utility that:
- connects to a SQL database,
- runs a query from `sql_query/`,
- cleans/transforms the results, and
- writes the cleaned report to a Google Sheet.

This README explains setup and how to run the script on Windows (PowerShell) using the project's layout.

---

## 🛠️ Prerequisites

Make sure you have:

1. Python 3.8+ installed and on PATH.
2. The `uv` installer if you prefer it (optional):

```powershell
# only if you want to install/upgrade uv
pip install -U uv
```

3. Project Python dependencies. If a `requirements.txt` or `pyproject.toml` exists, install via pip or your preferred tool. Example with pip and a requirements file:

```powershell
pip install -r requirements.txt
```

4. Access to the SQL database (host, database, user, password) used by the query in `sql_query/`.

5. Google Sheets API credentials (Service Account):
   - Create a Google Cloud project and enable the Google Sheets API.
   - Create a Service Account and download the JSON key (e.g. `service_account.json`).
   - Share the target Google Sheet with the service account email (give Editor access).

---

## Configuration

- Place your service account JSON somewhere on disk and point the environment variable `GOOGLE_APPLICATION_CREDENTIALS` at it. In PowerShell:

```powershell
$env:GOOGLE_APPLICATION_CREDENTIALS = 'C:\path\to\service_account.json'
```

- Supply DB credentials either via environment variables or by editing the configuration section the code expects (check `src/main.py` for exact config keys). If the project uses a config file, use that instead.

Notes / assumptions:
- I assume the main entry point is `src/main.py`. If your project uses a different module path, run that instead.

---

## Project layout

Top-level files and directories you should expect:

- `README.md` — this file.
- `pyproject.toml` — project metadata / dependencies (may exist).
- `sql_query/` — folder containing SQL query files used by the script.
- `src/` — Python source; expected entry `src/main.py`.

---

## How to run (Windows PowerShell)

Run the script directly with Python (recommended):

```powershell
# from project root
python src\main.py
```

Or run as a module:

```powershell
python -m src.main
```

If you use `uv` to manage environments or run scripts, adapt the above to your workflow (the repository doesn't require a specific runner beyond Python).

---

## Common tasks

- Update dependencies:

```powershell
pip install -r requirements.txt
```

- Set Google credentials (PowerShell):

```powershell
$env:GOOGLE_APPLICATION_CREDENTIALS = 'C:\full\path\to\service_account.json'
```

---

## Troubleshooting

- Google Sheets permission errors: confirm the service account email is shared with the sheet and has Editor rights.
- Database connection issues: verify network access, firewall, and provided DB credentials.
- Missing dependencies: install the packages listed in `requirements.txt` or shown in `pyproject.toml`.

---

## Real-Time Dashboard

This project includes a web-based dashboard that replaces the Google Sheets + Looker Studio pipeline with a self-hosted, near-real-time interface.

### What the Dashboard Does

Unlike the batch script (which runs every 15 minutes and writes to Google Sheets), the dashboard is a **web server** that:
- Starts once and stays running (like an app)
- Automatically queries the SQL database every **60 seconds**
- Displays results in a browser - anyone on the network can access it
- Only queries during business hours (6 AM - 6 PM)

### Dashboard Pages

| Page | URL | Description |
|------|-----|-------------|
| Case Locations | `/` | Main page - location grid with case counts + filterable data table |
| Workload | `/workload` | Stacked bar chart (Invoiced vs In Production) + category breakdown |
| Airway Workflow | `/airway` | Workflow stage cards (New Cases, Email, Zoom sections) |
| Airway Hold Status | `/airway-hold` | Hold status table with follow-up dates |
| Local Delivery | `/local-delivery` | Cases with LocalDelivery = TRUE |
| Overdue No Scan | `/overdue-noscan` | Overdue cases not scanned in 4 hours |

### How to Start the Dashboard

**Option 1: Double-click the batch file**

Double-click `run_dashboard.bat` in the project folder.

**Option 2: From a terminal**

```powershell
cd C:\Users\Partners\Desktop\repos\case_locations
uv run python -m dashboard.run
```

Once started, open a browser and go to:
- **On the same machine:** `http://localhost:8050`
- **From another computer on the network:** `http://<VM-IP-address>:8050`

You'll see a login page. The password is configured in the `.env` file (`DASHBOARD_PASSWORD`).

### How to Stop the Dashboard

- Close the terminal window, or
- Press `Ctrl+C` in the terminal

### Dashboard Configuration

Add these to your `.env` file:

```
DASHBOARD_PASSWORD=Partners1724!
DASHBOARD_PORT=8050
DASHBOARD_SECRET_KEY=your-secret-key-here
```

| Variable | Default | Description |
|----------|---------|-------------|
| `DASHBOARD_PASSWORD` | `Partners1724!` | Password for the login page |
| `DASHBOARD_PORT` | `8050` | Port number the dashboard runs on |
| `DASHBOARD_SECRET_KEY` | (auto) | Secret key for session cookies |

### Setting Up Auto-Start (Optional)

To have the dashboard start automatically at 6 AM every day:

1. Open Windows Task Scheduler
2. Create a new task:
   - **Trigger:** Daily at 6:00 AM
   - **Action:** Start a program: `C:\Users\Partners\Desktop\repos\case_locations\run_dashboard.bat`
   - **Settings:** Check "Run whether user is logged on or not"

The dashboard will automatically pause data queries outside of 6 AM - 6 PM.

### Filters

| Filter | What it Shows |
|--------|---------------|
| **Rush** | Cases where Pan Number starts with "R" and is less than 4 characters |
| **Leaves Today** | Cases with Ship Date = today |
| **Overdue** | Cases with Ship Date = previous business day, plus rush cases shipping today |
| **View All** | All cases (clears filters) |

### Company Holidays

Holidays are defined in `company_holidays.csv` at the project root. The file controls which days are skipped when calculating the previous business day (used by the Overdue and Leaves Today filters).

To add a holiday, open the CSV and add a new row:
```csv
2027-01-01,New Year's Day
```

The format is `YYYY-MM-DD,Holiday Name`. The dashboard will automatically pick up changes on the next data refresh (every 60 seconds).

### Network Access (Accessing from Other Computers)

The dashboard server binds to `0.0.0.0` (all network interfaces) by default. However, **Windows Firewall** blocks incoming connections. You must create a firewall rule to allow other computers on the network to access the dashboard.

**Option 1: PowerShell (Run as Administrator)**

```powershell
New-NetFirewallRule -DisplayName "Partners Dashboard (TCP 8050)" -Direction Inbound -Protocol TCP -LocalPort 8050 -Action Allow
```

**Option 2: Windows Firewall GUI**

1. Open **Windows Defender Firewall with Advanced Security** (search "wf.msc" in Start)
2. Click **Inbound Rules** in the left panel
3. Click **New Rule...** in the right panel
4. Choose **Port** -> Next
5. Choose **TCP**, enter **8050** in "Specific local ports" -> Next
6. Choose **Allow the connection** -> Next
7. Check all profiles (Domain, Private, Public) -> Next
8. Name it **Partners Dashboard (TCP 8050)** -> Finish

After creating the rule, other machines on the network can access `http://<server-IP>:8050`.

To find the server's IP address, run in PowerShell:
```powershell
ipconfig
```
Look for the **IPv4 Address** under your active network adapter (e.g., `192.168.10.x`).

### Troubleshooting

- **"Can't connect" / page won't load:** Check that the server is running. Check Windows Firewall allows port 8050 (see Network Access above).
- **"Data not updating":** Check SQL Server is accessible at 192.168.10.5. Look at the server terminal for error messages.
- **"Incorrect password":** Password is set in `.env` file as `DASHBOARD_PASSWORD`.
- **Change the port:** Edit `DASHBOARD_PORT` in `.env` and restart the server.

---