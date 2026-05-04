"""SQLite-backed storage for remake notes, L&D flags, and L&D email recipients.

Replaces the prior CSV/JSON files in User_Inputs/. Function signatures and
returned DataFrame column names match the legacy CSV-backed helpers so callers
don't need to change.

The DB lives at User_Inputs/dashboard.db and is created on first call to
init_db(). If the legacy CSV/JSON files exist at that point, their data is
imported (deduped by case_number) and the originals renamed to *.bak.
"""
import json
import logging
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import pandas as pd

logger = logging.getLogger("dashboard.data.notes_db")

_DB_PATH = Path(__file__).parent.parent.parent / "User_Inputs" / "dashboard.db"
_LEGACY_NOTES_CSV = Path(__file__).parent.parent.parent / "User_Inputs" / "remake_notes.csv"
_LEGACY_LD_CSV = Path(__file__).parent.parent.parent / "User_Inputs" / "remake_ld.csv"
_LEGACY_LD_EMAILS_JSON = Path(__file__).parent.parent.parent / "User_Inputs" / "ld_emails.json"

_LD_DEPTS = ["CS", "ThreeD", "Lab", "Shipping"]
_LD_DEPT_COL = {"CS": "cs", "ThreeD": "three_d", "Lab": "lab", "Shipping": "shipping"}
_LD_EMAILS_DEFAULT = {
    "CS":       ["nick@partnersdentalstudio.com"],
    "ThreeD":   ["nick@partnersdentalstudio.com"],
    "Lab":      ["nick@partnersdentalstudio.com"],
    "Shipping": ["nick@partnersdentalstudio.com"],
}

_write_lock = threading.Lock()

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS remake_notes (
    case_number    TEXT PRIMARY KEY,
    note           TEXT NOT NULL DEFAULT '',
    follow_up_note TEXT NOT NULL DEFAULT '',
    completed      INTEGER NOT NULL DEFAULT 0,
    last_updated   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS remake_ld (
    case_number  TEXT PRIMARY KEY,
    cs           INTEGER NOT NULL DEFAULT 0,
    three_d      INTEGER NOT NULL DEFAULT 0,
    lab          INTEGER NOT NULL DEFAULT 0,
    shipping     INTEGER NOT NULL DEFAULT 0,
    last_updated TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ld_emails (
    dept    TEXT PRIMARY KEY,
    emails  TEXT NOT NULL
);
"""


@contextmanager
def _conn():
    c = sqlite3.connect(_DB_PATH, isolation_level=None, timeout=10.0)
    try:
        c.execute("PRAGMA journal_mode=WAL;")
        c.execute("PRAGMA synchronous=NORMAL;")
        c.row_factory = sqlite3.Row
        yield c
    finally:
        c.close()


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def init_db() -> None:
    """Create schema if missing and import legacy CSV/JSON if present.

    Safe to call repeatedly — schema uses IF NOT EXISTS, and migration is
    skipped once the corresponding table is non-empty.
    """
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with _conn() as c:
        c.executescript(_SCHEMA_SQL)
    _migrate_notes_csv()
    _migrate_ld_csv()
    _migrate_ld_emails_json()


# ─── Migration ─────────────────────────────────────────────────────────────────

def _table_count(conn, table: str) -> int:
    return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def _migrate_notes_csv() -> None:
    if not _LEGACY_NOTES_CSV.exists():
        return
    with _conn() as c:
        if _table_count(c, "remake_notes") > 0:
            return
        try:
            df = pd.read_csv(_LEGACY_NOTES_CSV, dtype=str).fillna("")
        except Exception as e:
            logger.warning(f"Could not read legacy {_LEGACY_NOTES_CSV.name}: {e}")
            return
        for col, default in [("Note", ""), ("FollowUpNote", ""), ("Completed", "0"), ("LastUpdated", "")]:
            if col not in df.columns:
                df[col] = default
        df["MainCaseNumber"] = df["MainCaseNumber"].astype(str).str.strip()
        df = df[df["MainCaseNumber"] != ""]

        # Merge duplicate rows: longest non-empty text per col, max(Completed/LastUpdated).
        merged: dict[str, dict] = {}
        for row in df.to_dict("records"):
            cn = row["MainCaseNumber"]
            cur = merged.setdefault(cn, {"note": "", "follow_up_note": "", "completed": 0, "last_updated": ""})
            note_in = row.get("Note", "") or ""
            fu_in   = row.get("FollowUpNote", "") or ""
            comp_in = 1 if str(row.get("Completed", "0")).strip() == "1" else 0
            lu_in   = row.get("LastUpdated", "") or ""
            if len(note_in) > len(cur["note"]):
                cur["note"] = note_in
            if len(fu_in) > len(cur["follow_up_note"]):
                cur["follow_up_note"] = fu_in
            if comp_in > cur["completed"]:
                cur["completed"] = comp_in
            if lu_in > cur["last_updated"]:
                cur["last_updated"] = lu_in

        rows = [
            (cn, v["note"], v["follow_up_note"], v["completed"], v["last_updated"] or _now())
            for cn, v in merged.items()
        ]
        c.executemany(
            "INSERT INTO remake_notes(case_number, note, follow_up_note, completed, last_updated) "
            "VALUES(?, ?, ?, ?, ?)",
            rows,
        )
    _archive_legacy(_LEGACY_NOTES_CSV)
    logger.info(f"Migrated {len(merged)} cases from remake_notes.csv into SQLite (deduped from {len(df)} rows)")


def _migrate_ld_csv() -> None:
    if not _LEGACY_LD_CSV.exists():
        return
    with _conn() as c:
        if _table_count(c, "remake_ld") > 0:
            return
        try:
            df = pd.read_csv(_LEGACY_LD_CSV, dtype=str).fillna("")
        except Exception as e:
            logger.warning(f"Could not read legacy {_LEGACY_LD_CSV.name}: {e}")
            return
        for col, default in [("CS", "0"), ("ThreeD", "0"), ("Lab", "0"), ("Shipping", "0"), ("LastUpdated", "")]:
            if col not in df.columns:
                df[col] = default
        df["MainCaseNumber"] = df["MainCaseNumber"].astype(str).str.strip()
        df = df[df["MainCaseNumber"] != ""]

        merged: dict[str, dict] = {}
        for row in df.to_dict("records"):
            cn = row["MainCaseNumber"]
            cur = merged.setdefault(cn, {"cs": 0, "three_d": 0, "lab": 0, "shipping": 0, "last_updated": ""})
            for dept, col in _LD_DEPT_COL.items():
                v = 1 if str(row.get(dept, "0")).strip() == "1" else 0
                if v > cur[col]:
                    cur[col] = v
            lu_in = row.get("LastUpdated", "") or ""
            if lu_in > cur["last_updated"]:
                cur["last_updated"] = lu_in

        rows = [
            (cn, v["cs"], v["three_d"], v["lab"], v["shipping"], v["last_updated"] or _now())
            for cn, v in merged.items()
        ]
        c.executemany(
            "INSERT INTO remake_ld(case_number, cs, three_d, lab, shipping, last_updated) "
            "VALUES(?, ?, ?, ?, ?, ?)",
            rows,
        )
    _archive_legacy(_LEGACY_LD_CSV)
    logger.info(f"Migrated {len(merged)} cases from remake_ld.csv into SQLite (deduped from {len(df)} rows)")


def _migrate_ld_emails_json() -> None:
    if not _LEGACY_LD_EMAILS_JSON.exists():
        return
    with _conn() as c:
        if _table_count(c, "ld_emails") > 0:
            return
        try:
            data = json.loads(_LEGACY_LD_EMAILS_JSON.read_text())
        except Exception as e:
            logger.warning(f"Could not read legacy {_LEGACY_LD_EMAILS_JSON.name}: {e}")
            return
        if not isinstance(data, dict):
            return
        rows = [(dept, json.dumps(emails or [])) for dept, emails in data.items() if dept in _LD_DEPTS]
        c.executemany("INSERT INTO ld_emails(dept, emails) VALUES(?, ?)", rows)
    _archive_legacy(_LEGACY_LD_EMAILS_JSON)
    logger.info("Migrated ld_emails.json into SQLite")


def _archive_legacy(path: Path) -> None:
    try:
        path.rename(path.with_suffix(path.suffix + ".bak"))
    except Exception as e:
        logger.warning(f"Could not rename {path.name} to .bak: {e}")


# ─── Notes ────────────────────────────────────────────────────────────────────

def save_remake_note(case_number, note_text: str) -> None:
    cn = str(case_number).strip()
    if not cn:
        return
    with _write_lock, _conn() as c:
        c.execute(
            """
            INSERT INTO remake_notes(case_number, note, last_updated)
            VALUES(?, ?, ?)
            ON CONFLICT(case_number) DO UPDATE SET
                note = excluded.note,
                last_updated = excluded.last_updated
            """,
            (cn, note_text or "", _now()),
        )


def save_follow_up_note(case_number, note_text: str) -> None:
    cn = str(case_number).strip()
    if not cn:
        return
    with _write_lock, _conn() as c:
        c.execute(
            """
            INSERT INTO remake_notes(case_number, follow_up_note, last_updated)
            VALUES(?, ?, ?)
            ON CONFLICT(case_number) DO UPDATE SET
                follow_up_note = excluded.follow_up_note,
                last_updated = excluded.last_updated
            """,
            (cn, note_text or "", _now()),
        )


def save_case_completed(case_number, completed: bool) -> None:
    cn = str(case_number).strip()
    if not cn:
        return
    with _write_lock, _conn() as c:
        c.execute(
            """
            INSERT INTO remake_notes(case_number, completed, last_updated)
            VALUES(?, ?, ?)
            ON CONFLICT(case_number) DO UPDATE SET
                completed = excluded.completed,
                last_updated = excluded.last_updated
            """,
            (cn, 1 if completed else 0, _now()),
        )


def load_remake_notes() -> pd.DataFrame:
    with _conn() as c:
        rows = c.execute(
            """
            SELECT case_number   AS MainCaseNumber,
                   note          AS Note,
                   follow_up_note AS FollowUpNote,
                   CAST(completed AS TEXT) AS Completed,
                   last_updated  AS LastUpdated
            FROM remake_notes
            """
        ).fetchall()
    return pd.DataFrame(
        [dict(r) for r in rows],
        columns=["MainCaseNumber", "Note", "FollowUpNote", "Completed", "LastUpdated"],
    )


# ─── L&D flags ────────────────────────────────────────────────────────────────

def save_remake_ld(case_number, dept: str, checked: bool) -> None:
    if dept not in _LD_DEPT_COL:
        raise ValueError(f"Unknown dept: {dept}")
    cn = str(case_number).strip()
    if not cn:
        return
    col = _LD_DEPT_COL[dept]
    val = 1 if checked else 0
    with _write_lock, _conn() as c:
        c.execute(
            f"""
            INSERT INTO remake_ld(case_number, {col}, last_updated)
            VALUES(?, ?, ?)
            ON CONFLICT(case_number) DO UPDATE SET
                {col} = excluded.{col},
                last_updated = excluded.last_updated
            """,
            (cn, val, _now()),
        )


def load_remake_ld() -> pd.DataFrame:
    with _conn() as c:
        rows = c.execute(
            """
            SELECT case_number AS MainCaseNumber,
                   CAST(cs       AS TEXT) AS CS,
                   CAST(three_d  AS TEXT) AS ThreeD,
                   CAST(lab      AS TEXT) AS Lab,
                   CAST(shipping AS TEXT) AS Shipping,
                   last_updated AS LastUpdated
            FROM remake_ld
            """
        ).fetchall()
    return pd.DataFrame(
        [dict(r) for r in rows],
        columns=["MainCaseNumber", "CS", "ThreeD", "Lab", "Shipping", "LastUpdated"],
    )


# ─── L&D email recipients ─────────────────────────────────────────────────────

def load_ld_emails() -> dict:
    with _conn() as c:
        rows = c.execute("SELECT dept, emails FROM ld_emails").fetchall()
    out = {}
    for r in rows:
        try:
            out[r["dept"]] = json.loads(r["emails"]) or []
        except Exception:
            out[r["dept"]] = []
    # Ensure every known dept has an entry
    for d in _LD_DEPTS:
        out.setdefault(d, list(_LD_EMAILS_DEFAULT.get(d, [])))
    return out


def save_ld_emails(emails: dict) -> None:
    if not isinstance(emails, dict):
        return
    rows = [(dept, json.dumps(addrs or [])) for dept, addrs in emails.items() if dept in _LD_DEPTS]
    with _write_lock, _conn() as c:
        c.executemany(
            """
            INSERT INTO ld_emails(dept, emails) VALUES(?, ?)
            ON CONFLICT(dept) DO UPDATE SET emails = excluded.emails
            """,
            rows,
        )
