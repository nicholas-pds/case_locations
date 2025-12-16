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
