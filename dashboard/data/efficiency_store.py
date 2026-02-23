"""Parquet-based persistence for efficiency data."""
import logging
from pathlib import Path
import pandas as pd

logger = logging.getLogger("dashboard.efficiency_store")

# Storage directory: dashboard/data/efficiency_data/
_DATA_DIR = Path(__file__).parent / "efficiency_data"
_DATA_DIR.mkdir(exist_ok=True)

_DAILY_PATH = _DATA_DIR / "daily.parquet"
_AGGREGATED_PATH = _DATA_DIR / "aggregated.parquet"
_NOON_PATH = _DATA_DIR / "noon.parquet"
_3PM_PATH = _DATA_DIR / "3pm.parquet"

# Tech constants CSV
_CONSTANTS_PATH = Path(__file__).parent.parent.parent / "User_Inputs" / "tech_constants.csv"


_TEAM_FIX_NAMES = {
    "Albert Cherniavskyi", "Andrii Mishyn", "Don William",
    "Olena Shypilova", "Vlad Dorofieiev",
}


def _fix_team_assignments(df: pd.DataFrame) -> pd.DataFrame:
    """Fix records where Team is '0' or 'O' for specific employees."""
    if df.empty or "Team" not in df.columns:
        return df
    name_col = "MT Name" if "MT Name" in df.columns else "Name" if "Name" in df.columns else None
    if name_col is None:
        return df
    mask = df["Team"].isin(["0", "O"]) & df[name_col].isin(_TEAM_FIX_NAMES)
    if mask.any():
        df = df.copy()
        df.loc[mask, "Team"] = "z_Not On Report"
    return df


def _safe_read(path: Path) -> pd.DataFrame:
    if path.exists():
        try:
            return pd.read_parquet(path)
        except Exception as e:
            logger.warning(f"Failed to read {path}: {e}")
    return pd.DataFrame()


def load_daily() -> pd.DataFrame:
    return _fix_team_assignments(_safe_read(_DAILY_PATH))


def save_daily(df: pd.DataFrame) -> None:
    df.to_parquet(_DAILY_PATH, index=False)
    logger.info(f"Saved daily efficiency data: {len(df)} rows")


def load_aggregated() -> pd.DataFrame:
    df = _safe_read(_AGGREGATED_PATH)
    if not df.empty:
        # Efficiency columns stored as str — convert numeric values back to float, keep "x"
        for col in df.columns:
            if col.startswith("Efficiency_"):
                df[col] = df[col].apply(
                    lambda v: float(v) if v not in ("x", "nan", "None", "") else v
                )
    return _fix_team_assignments(df)


def save_aggregated(df: pd.DataFrame) -> None:
    # Efficiency columns have mixed types (float + "x" string) — cast to str for parquet
    out = df.copy()
    for col in out.columns:
        if col.startswith("Efficiency_"):
            out[col] = out[col].astype(str)
    out.to_parquet(_AGGREGATED_PATH, index=False)
    logger.info(f"Saved aggregated efficiency data: {len(df)} rows")


def load_midday(window: str) -> pd.DataFrame:
    """window: 'noon' or '3pm'"""
    path = _NOON_PATH if window == "noon" else _3PM_PATH
    return _fix_team_assignments(_safe_read(path))


def save_midday(window: str, df: pd.DataFrame) -> None:
    """window: 'noon' or '3pm'"""
    path = _NOON_PATH if window == "noon" else _3PM_PATH
    df.to_parquet(path, index=False)
    logger.info(f"Saved {window} midday data: {len(df)} rows")


# ─────────────────────────────────────────────
# Tech Constants (CSV-based)
# ─────────────────────────────────────────────

def load_tech_constants() -> pd.DataFrame:
    """Load tech constants CSV. Returns DataFrame with Name, Noon, 3PM, ShiftType, DesignType."""
    if _CONSTANTS_PATH.exists():
        try:
            df = pd.read_csv(_CONSTANTS_PATH, dtype=str)
            # Keep Noon/3PM as strings to preserve "NA" values
            return df
        except Exception as e:
            logger.warning(f"Failed to read tech constants: {e}")
    return pd.DataFrame(columns=["Name", "Noon", "3PM", "ShiftType", "DesignType"])


def save_tech_constants(df: pd.DataFrame) -> None:
    """Write tech constants DataFrame back to CSV."""
    df.to_csv(_CONSTANTS_PATH, index=False)
    logger.info(f"Saved tech constants: {len(df)} rows")
