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


def _safe_read(path: Path) -> pd.DataFrame:
    if path.exists():
        try:
            return pd.read_parquet(path)
        except Exception as e:
            logger.warning(f"Failed to read {path}: {e}")
    return pd.DataFrame()


def load_daily() -> pd.DataFrame:
    return _safe_read(_DAILY_PATH)


def save_daily(df: pd.DataFrame) -> None:
    df.to_parquet(_DAILY_PATH, index=False)
    logger.info(f"Saved daily efficiency data: {len(df)} rows")


def load_aggregated() -> pd.DataFrame:
    return _safe_read(_AGGREGATED_PATH)


def save_aggregated(df: pd.DataFrame) -> None:
    df.to_parquet(_AGGREGATED_PATH, index=False)
    logger.info(f"Saved aggregated efficiency data: {len(df)} rows")


def load_midday(window: str) -> pd.DataFrame:
    """window: 'noon' or '3pm'"""
    path = _NOON_PATH if window == "noon" else _3PM_PATH
    return _safe_read(path)


def save_midday(window: str, df: pd.DataFrame) -> None:
    """window: 'noon' or '3pm'"""
    path = _NOON_PATH if window == "noon" else _3PM_PATH
    df.to_parquet(path, index=False)
    logger.info(f"Saved {window} midday data: {len(df)} rows")
