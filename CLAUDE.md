# Claude Instructions

## After completing any code changes
Always ask the user if they want to commit and push before ending your response.

---

## Project Overview

**What it is**: FastAPI + Jinja2 + Alpine.js + htmx internal dashboard for Partners Dental.

**How to run**: `python dashboard/run.py`

### Key Structure
- `dashboard/app.py` — app factory (`create_app()`), Jinja2 filters (`fmt_time`, `fmt_date`, `fmt_datetime`)
- `dashboard/routes/` — route handlers per module
- `dashboard/templates/` — Jinja2 templates (all extend `base.html`)
- `dashboard/data/` — data layer (cache, queries, parquet store)
- `src/db_handler.py` — SQL Server connection via pyodbc (`get_sql_server_credentials()`)
- `User_Inputs/*.csv` — user-managed lookup/config files (tech_constants, employee_lkups, revenue_goals, remake_notes)

### Modules
| Route | Description |
|---|---|
| `/remakes` | Tracks dental remake cases — SQL, 180-day window, Alpine.js tabs + Chart.js |
| `/efficiency` | 12-week rolling efficiency per team/employee (parquet-backed) |
| `/daily-summary` | Daily summary view |

### Cache
`dashboard/data/cache.py` — async `DataCache` singleton (`cache`); use `await cache.set(key, val)`, `await cache.get(key)`, `cache.get_sync(key)`.

### CSS Palette
`--red: #c41227; --gray: #6B6B6B; --dark: #1A1A1A; --light: #F8F8F8; --border: #E5E5E5;`
