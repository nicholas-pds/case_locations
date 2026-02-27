"""CSV-based persistence for revenue goals."""
import logging
from pathlib import Path
import pandas as pd

logger = logging.getLogger("dashboard.revenue_goals_store")

_GOALS_PATH = Path(__file__).parent.parent.parent / "User_Inputs" / "revenue_goals.csv"

_GOALS_COLS = ["Year", "Month", "RevenueGoal"]


def load_revenue_goals() -> pd.DataFrame:
    """Load revenue goals CSV. Returns DataFrame with Year, Month, RevenueGoal columns."""
    if _GOALS_PATH.exists():
        try:
            return pd.read_csv(_GOALS_PATH, dtype={"Year": int, "Month": int, "RevenueGoal": float})
        except Exception as e:
            logger.warning(f"Failed to read revenue goals: {e}")
    return pd.DataFrame(columns=_GOALS_COLS)


def save_revenue_goals(df: pd.DataFrame) -> None:
    """Write revenue goals DataFrame back to CSV."""
    out = df.copy()
    out["Year"] = out["Year"].astype(int)
    out["Month"] = out["Month"].astype(int)
    out["RevenueGoal"] = out["RevenueGoal"].astype(float)
    out.to_csv(_GOALS_PATH, index=False)
    logger.info(f"Saved revenue goals: {len(df)} rows")
