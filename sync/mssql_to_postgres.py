#!/usr/bin/env python3
"""sync/mssql_to_postgres.py

Syncs MagicTouch SQL Server → PostgreSQL (Partners DS schema).

Required .env variables:
  MSSQL_SERVER    — SQL Server host (e.g. 192.168.1.10 or server\\instance)
  MSSQL_DATABASE  — Database name
  MSSQL_USER      — SQL auth username
  MSSQL_PASSWORD  — SQL auth password
  POSTGRES_URL    — Full connection string, e.g. postgresql://user:pass@host/db

Usage:
  python sync/mssql_to_postgres.py                  # sync all tables
  python sync/mssql_to_postgres.py --table cases    # sync one table

NOTE: dbo.Departments and dbo.Employees query shapes assume a typical MagicTouch
schema.  Adjust column names in _QUERIES if your instance differs.
"""

import argparse
import os
import sys

# Ensure UTF-8 output on Windows consoles that default to cp1252
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

import psycopg2
import psycopg2.extras
import pyodbc
from dotenv import load_dotenv

load_dotenv()

# ─── Table order ──────────────────────────────────────────────────────────────

_BATCH_CASES = 500  # cases per commit for delete+re-insert tables

SYNC_ORDER = [
    "case_statuses",
    "departments",
    "employees",
    "customers",
    "cases",
    "case_tasks",
    "case_tasks_history",
]

# ─── MSSQL queries ────────────────────────────────────────────────────────────
# Edit these if your MagicTouch column names differ.

_SQL_CASE_STATUSES = """
    SELECT DISTINCT LTRIM(RTRIM([Status])) AS Status
    FROM dbo.Cases
    WHERE [Status] IS NOT NULL AND LTRIM(RTRIM([Status])) <> ''
"""

_SQL_DEPARTMENTS = """
    SELECT DepartmentID, DepartmentName, DeptCode, Active
    FROM dbo.Departments
"""

_SQL_EMPLOYEES = """
    SELECT EmployeeID, FirstName, LastName, Department, Position, Active, HireDate
    FROM dbo.Employees
    WHERE Deleted = 0 OR Deleted IS NULL
"""

_SQL_CUSTOMERS = """
    SELECT c.CustomerID, c.PracticeName, c.SalesPerson,
           c.Address1, c.Address2, c.City, c.[State], c.ZipCode,
           c.OfficePhone, c.Email, c.Active
    FROM dbo.Customers c
    WHERE c.Deleted = 0
"""

_SQL_CASES = """
    SELECT c.CaseNumber,
           c.[Status],
           c.CustomerID,
           CAST(c.DateIn      AS DATE) AS DateIn,
           CAST(c.ShipDate    AS DATE) AS ShipDate,
           CAST(c.DueDate     AS DATE) AS DueDate,
           CAST(c.InvoiceDate AS DATE) AS InvoiceDate,
           c.TotalCharge,
           c.RemakeReason,
           c.Remake,
           c.PanNumber,
           c.WorkOrderNotes
    FROM dbo.Cases c
    WHERE c.DateIn >= DATEADD(DAY, -180, CAST(GETDATE() AS DATE))
      AND c.Deleted = 0
"""

_SQL_CASE_TASKS = """
    SELECT ca.CaseNumber, ct.Task, ct.CompleteDate, ct.Duration
    FROM dbo.CaseTasks ct
    INNER JOIN dbo.Cases ca ON ca.CaseID = ct.CaseID
    WHERE ca.DateIn >= DATEADD(DAY, -180, CAST(GETDATE() AS DATE))
      AND ca.Deleted = 0
"""

_SQL_CASE_TASKS_HISTORY = """
    SELECT ca.CaseNumber, cth.Task, cth.CompleteDate,
           cth.CompletedBY, cth.Rejected
    FROM dbo.CaseTasksHistory cth
    INNER JOIN dbo.Cases ca ON ca.CaseID = cth.CaseID
    WHERE ca.DateIn >= DATEADD(DAY, -180, CAST(GETDATE() AS DATE))
      AND ca.Deleted = 0
"""

# ─── Connections ──────────────────────────────────────────────────────────────

def _env(primary: str, fallback: str) -> str:
    """Read primary env var, fall back to legacy name, raise if neither set."""
    val = os.environ.get(primary) or os.environ.get(fallback)
    if not val:
        raise KeyError(f"Neither {primary} nor {fallback} is set in .env")
    return val


def get_mssql_conn() -> pyodbc.Connection:
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={_env('MSSQL_SERVER',   'SQL_SERVER')};"
        f"DATABASE={_env('MSSQL_DATABASE', 'SQL_DATABASE')};"
        f"UID={_env('MSSQL_USER',     'SQL_USERNAME')};"
        f"PWD={_env('MSSQL_PASSWORD', 'SQL_PASSWORD')}"
    )
    return pyodbc.connect(conn_str)


def get_pg_conn() -> psycopg2.extensions.connection:
    return psycopg2.connect(os.environ["POSTGRES_URL"])


# ─── MSSQL helpers ────────────────────────────────────────────────────────────

def mssql_fetchall(conn: pyodbc.Connection, sql: str) -> list[tuple]:
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    return rows


# ─── sync_log helpers ─────────────────────────────────────────────────────────

def start_sync_log(pg_conn, table_name: str) -> int:
    """Insert a 'running' row and return sync_id."""
    with pg_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO sync_log (sync_type, status) VALUES (%s, 'running') RETURNING sync_id",
            (table_name,),
        )
        sync_id = cur.fetchone()[0]
    pg_conn.commit()
    return sync_id


def finish_sync_log(
    pg_conn,
    sync_id: int,
    rows_attempted: int,
    rows_succeeded: int,
    error_msg: str | None = None,
) -> None:
    status = "success" if error_msg is None else "failed"
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            UPDATE sync_log
               SET status            = %s,
                   records_attempted = %s,
                   records_succeeded = %s,
                   records_failed    = %s,
                   error_message     = %s,
                   completed_at      = NOW()
             WHERE sync_id = %s
            """,
            (
                status,
                rows_attempted,
                rows_succeeded,
                rows_attempted - rows_succeeded,
                error_msg,
                sync_id,
            ),
        )
    pg_conn.commit()


# ─── PG lookup builders ───────────────────────────────────────────────────────

def _build_lookup(pg_conn, table: str, code_col: str, id_col: str) -> dict:
    with pg_conn.cursor() as cur:
        cur.execute(f"SELECT {code_col}, {id_col} FROM {table}")
        return {row[0]: row[1] for row in cur.fetchall() if row[0] is not None}


def build_status_lookup(pg_conn)   -> dict: return _build_lookup(pg_conn, "case_statuses", "status_name",   "status_id")
def build_dept_lookup(pg_conn)     -> dict: return _build_lookup(pg_conn, "departments",   "department_name","department_id")
def build_emp_lookup(pg_conn)      -> dict: return _build_lookup(pg_conn, "employees",     "employee_code",  "employee_id")
def build_customer_lookup(pg_conn) -> dict: return _build_lookup(pg_conn, "customers",     "customer_code",  "customer_id")
def build_case_lookup(pg_conn)     -> dict: return _build_lookup(pg_conn, "cases",         "case_number",    "case_id")


# ─── Table sync functions ─────────────────────────────────────────────────────

def sync_case_statuses(mssql_conn, pg_conn) -> int:
    rows = mssql_fetchall(mssql_conn, _SQL_CASE_STATUSES)
    records = [(row[0],) for row in rows if row[0]]

    with pg_conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO case_statuses (status_name)
            VALUES %s
            ON CONFLICT (status_name) DO UPDATE SET is_active = TRUE
            """,
            records,
        )
    pg_conn.commit()
    return len(records)


def sync_departments(mssql_conn, pg_conn) -> int:
    try:
        rows = mssql_fetchall(mssql_conn, _SQL_DEPARTMENTS)
    except Exception:
        print(
            "  [departments] dbo.Departments not found — "
            "skipping (pre-seeded PG values unchanged)",
            flush=True,
        )
        return 0

    # (department_code, department_name, is_active)
    records = [
        (str(r[0]), str(r[1]), bool(r[3]) if r[3] is not None else True)
        for r in rows
    ]

    with pg_conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO departments (department_code, department_name, is_active)
            VALUES %s
            ON CONFLICT (department_name) DO UPDATE
                SET department_code = EXCLUDED.department_code,
                    is_active       = EXCLUDED.is_active
            """,
            records,
        )
    pg_conn.commit()
    return len(records)


def sync_employees(mssql_conn, pg_conn, dept_lookup: dict) -> int:
    try:
        rows = mssql_fetchall(mssql_conn, _SQL_EMPLOYEES)
    except Exception as e:
        raise RuntimeError(f"dbo.Employees query failed: {e}") from e

    # (employee_code, first_name, last_name, department_id, role, is_active, hire_date)
    records = [
        (
            str(r[0]),                                        # employee_code = EmployeeID
            r[1] or "",                                       # first_name
            r[2] or "",                                       # last_name
            dept_lookup.get(r[3]) if r[3] else None,         # department_id via dept name
            r[4],                                             # role
            bool(r[5]) if r[5] is not None else True,        # is_active
            r[6],                                             # hire_date
        )
        for r in rows
    ]

    with pg_conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO employees
                (employee_code, first_name, last_name, department_id, role, is_active, hire_date)
            VALUES %s
            ON CONFLICT (employee_code) DO UPDATE
                SET first_name    = EXCLUDED.first_name,
                    last_name     = EXCLUDED.last_name,
                    department_id = EXCLUDED.department_id,
                    role          = EXCLUDED.role,
                    is_active     = EXCLUDED.is_active,
                    hire_date     = EXCLUDED.hire_date
            """,
            records,
        )
    pg_conn.commit()
    return len(records)


def sync_customers(mssql_conn, pg_conn) -> int:
    rows = mssql_fetchall(mssql_conn, _SQL_CUSTOMERS)

    # (customer_code, practice_name, salesperson_id,
    #  address_line1, address_line2, city, state, zip, phone, email, is_active)
    # salesperson_id left NULL — SalesPerson in MagicTouch is a name string,
    # not a FK; resolve in a follow-up query once employees are imported if needed.
    records = [
        (
            str(r[0]),                                        # customer_code = CustomerID
            r[1] or "",                                       # practice_name
            None,                                             # salesperson_id
            r[3], r[4], r[5],                                # address_line1/2, city
            r[6] if r[6] and len(str(r[6]).strip()) <= 2     # state — NULL if not a 2-char code
                else None,
            r[7],                                             # zip
            r[8], r[9],                                       # phone, email
            bool(r[10]) if r[10] is not None else True,      # is_active
        )
        for r in rows
    ]

    with pg_conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO customers
                (customer_code, practice_name, salesperson_id,
                 address_line1, address_line2, city, state, zip,
                 phone, email, is_active)
            VALUES %s
            ON CONFLICT (customer_code) DO UPDATE
                SET practice_name = EXCLUDED.practice_name,
                    address_line1 = EXCLUDED.address_line1,
                    address_line2 = EXCLUDED.address_line2,
                    city          = EXCLUDED.city,
                    state         = EXCLUDED.state,
                    zip           = EXCLUDED.zip,
                    phone         = EXCLUDED.phone,
                    email         = EXCLUDED.email,
                    is_active     = EXCLUDED.is_active
            """,
            records,
        )
    pg_conn.commit()
    return len(records)


def sync_cases(
    mssql_conn,
    pg_conn,
    status_lookup: dict,
    customer_lookup: dict,
) -> int:
    rows = mssql_fetchall(mssql_conn, _SQL_CASES)

    # (case_number, is_remake, remake_reason, customer_id, status_id,
    #  pan_number, due_date, received_date, invoice_date, ship_date, invoice_amount, notes)
    records = [
        (
            str(r[0]),                                           # case_number
            bool(r[9] and str(r[9]).strip()),                    # is_remake (Remake column non-empty)
            r[8],                                                # remake_reason
            customer_lookup.get(str(r[2])),                      # customer_id
            status_lookup.get(str(r[1])) if r[1] else None,     # status_id
            r[10],                                               # pan_number
            r[6],                                                # due_date
            r[3],                                                # received_date = DateIn
            r[6],                                                # invoice_date
            r[4],                                                # ship_date
            r[7],                                                # invoice_amount = TotalCharge
            r[11],                                               # notes
        )
        for r in rows
    ]

    with pg_conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO cases
                (case_number, is_remake, remake_reason, customer_id, status_id,
                 pan_number, due_date, received_date, invoice_date, ship_date,
                 invoice_amount, notes)
            VALUES %s
            ON CONFLICT (case_number) DO UPDATE
                SET is_remake      = EXCLUDED.is_remake,
                    remake_reason  = EXCLUDED.remake_reason,
                    customer_id    = EXCLUDED.customer_id,
                    status_id      = EXCLUDED.status_id,
                    pan_number     = EXCLUDED.pan_number,
                    due_date       = EXCLUDED.due_date,
                    received_date  = EXCLUDED.received_date,
                    invoice_date   = EXCLUDED.invoice_date,
                    ship_date      = EXCLUDED.ship_date,
                    invoice_amount = EXCLUDED.invoice_amount,
                    notes          = EXCLUDED.notes
            """,
            records,
        )
    pg_conn.commit()
    return len(records)


def sync_case_tasks(mssql_conn, pg_conn, case_lookup: dict) -> int:
    # case_tasks has no stable natural key in MagicTouch; we delete + re-insert
    # all tasks for the synced cases so data stays current.
    rows = mssql_fetchall(mssql_conn, _SQL_CASE_TASKS)

    # Group by PG case_id
    by_case: dict[int, list[tuple]] = {}
    skipped = 0
    for r in rows:
        case_num, task, complete_date, duration = r[0], r[1], r[2], r[3]
        case_id = case_lookup.get(str(case_num))
        if not case_id:
            skipped += 1
            continue
        # (case_id, task_name, completed_at, description)
        by_case.setdefault(case_id, []).append(
            (case_id, str(task) if task else "Unknown", complete_date, None)
        )

    if skipped:
        print(f"  [case_tasks] {skipped} rows skipped (case not in PG yet)", flush=True)

    if not by_case:
        return 0

    total = 0
    case_ids = list(by_case.keys())
    for i in range(0, len(case_ids), _BATCH_CASES):
        chunk_ids = case_ids[i : i + _BATCH_CASES]
        chunk_records = [rec for cid in chunk_ids for rec in by_case[cid]]
        with pg_conn.cursor() as cur:
            cur.execute("DELETE FROM case_tasks WHERE case_id = ANY(%s)", (chunk_ids,))
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO case_tasks (case_id, task_name, completed_at, description) VALUES %s",
                chunk_records,
            )
        pg_conn.commit()
        total += len(chunk_records)
        print(f"  [case_tasks] batch {i // _BATCH_CASES + 1}: {total} rows so far", flush=True)
    return total


def sync_case_tasks_history(
    mssql_conn,
    pg_conn,
    case_lookup: dict,
    emp_lookup: dict,
) -> int:
    # Same delete + re-insert strategy as case_tasks.
    rows = mssql_fetchall(mssql_conn, _SQL_CASE_TASKS_HISTORY)

    by_case: dict[int, list[tuple]] = {}
    skipped = 0
    for r in rows:
        case_num, task, complete_date, completed_by, rejected = r
        case_id = case_lookup.get(str(case_num))
        if not case_id:
            skipped += 1
            continue
        emp_id = emp_lookup.get(str(completed_by)) if completed_by else None
        action = "Rejected" if rejected else "Completed"
        # (case_id, task_id, department_id, employee_id,
        #  task_name, action, old_status, new_status, notes, action_at)
        by_case.setdefault(case_id, []).append(
            (case_id, None, None, emp_id, task, action, None, None, None, complete_date)
        )

    if skipped:
        print(f"  [case_tasks_history] {skipped} rows skipped (case not in PG yet)", flush=True)

    if not by_case:
        return 0

    total = 0
    case_ids = list(by_case.keys())
    for i in range(0, len(case_ids), _BATCH_CASES):
        chunk_ids = case_ids[i : i + _BATCH_CASES]
        chunk_records = [rec for cid in chunk_ids for rec in by_case[cid]]
        with pg_conn.cursor() as cur:
            cur.execute("DELETE FROM case_tasks_history WHERE case_id = ANY(%s)", (chunk_ids,))
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO case_tasks_history
                    (case_id, task_id, department_id, employee_id,
                     task_name, action, old_status, new_status, notes, action_at)
                VALUES %s
                """,
                chunk_records,
            )
        pg_conn.commit()
        total += len(chunk_records)
        print(f"  [case_tasks_history] batch {i // _BATCH_CASES + 1}: {total} rows so far", flush=True)
    return total


# ─── Orchestration ────────────────────────────────────────────────────────────

def run_sync(table_filter: str | None = None) -> None:
    tables = [table_filter] if table_filter else SYNC_ORDER

    server = _env('MSSQL_SERVER', 'SQL_SERVER')
    db     = _env('MSSQL_DATABASE', 'SQL_DATABASE')
    print(f"Connecting to SQL Server ({server}/{db})...", flush=True)
    mssql_conn = get_mssql_conn()

    print("Connecting to PostgreSQL...", flush=True)
    pg_conn = get_pg_conn()

    # Build FK lookup dicts from PG before we start (handles --table partial runs)
    print("Loading existing PG lookup tables...", flush=True)
    status_lookup   = build_status_lookup(pg_conn)
    dept_lookup     = build_dept_lookup(pg_conn)
    emp_lookup      = build_emp_lookup(pg_conn)
    customer_lookup = build_customer_lookup(pg_conn)
    case_lookup     = build_case_lookup(pg_conn)

    try:
        for table in tables:
            print(f"\n→ Syncing {table}...", flush=True)
            sync_id = start_sync_log(pg_conn, table)
            try:
                if table == "case_statuses":
                    n = sync_case_statuses(mssql_conn, pg_conn)
                    status_lookup = build_status_lookup(pg_conn)

                elif table == "departments":
                    n = sync_departments(mssql_conn, pg_conn)
                    dept_lookup = build_dept_lookup(pg_conn)

                elif table == "employees":
                    n = sync_employees(mssql_conn, pg_conn, dept_lookup)
                    emp_lookup = build_emp_lookup(pg_conn)

                elif table == "customers":
                    n = sync_customers(mssql_conn, pg_conn)
                    customer_lookup = build_customer_lookup(pg_conn)

                elif table == "cases":
                    n = sync_cases(mssql_conn, pg_conn, status_lookup, customer_lookup)
                    case_lookup = build_case_lookup(pg_conn)

                elif table == "case_tasks":
                    n = sync_case_tasks(mssql_conn, pg_conn, case_lookup)

                elif table == "case_tasks_history":
                    n = sync_case_tasks_history(mssql_conn, pg_conn, case_lookup, emp_lookup)

                else:
                    raise ValueError(f"Unknown table: {table}")

                finish_sync_log(pg_conn, sync_id, rows_attempted=n, rows_succeeded=n)
                print(f"  ✓ {n} rows upserted", flush=True)

            except Exception as exc:
                try:
                    pg_conn.rollback()
                    finish_sync_log(pg_conn, sync_id, rows_attempted=0, rows_succeeded=0, error_msg=str(exc))
                except Exception:
                    pass
                print(f"  ERROR: {exc}", file=sys.stderr, flush=True)
                if table_filter:
                    # Single-table run: propagate error so caller sees exit code 1
                    raise
                # Full run: log and continue with remaining tables

    finally:
        mssql_conn.close()
        pg_conn.close()

    print("\nSync complete.", flush=True)


# ─── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Sync MagicTouch SQL Server → PostgreSQL (Partners DS schema)"
    )
    parser.add_argument(
        "--table",
        choices=SYNC_ORDER,
        metavar="TABLE",
        help=f"Sync a single table instead of all. Choices: {', '.join(SYNC_ORDER)}",
    )
    args = parser.parse_args()

    try:
        run_sync(table_filter=args.table)
    except Exception as e:
        print(f"\nFatal: {e}", file=sys.stderr)
        sys.exit(1)
