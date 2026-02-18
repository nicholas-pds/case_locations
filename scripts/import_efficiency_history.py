"""
One-time script to import historical efficiency data from eff_all.xlsx into parquet storage.
Run from project root: python scripts/import_efficiency_history.py
"""
import sys
from pathlib import Path

# Add project root to path so imports work
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import pandas as pd
from dashboard.data.efficiency_store import save_daily, save_aggregated
from dashboard.data.efficiency_processing import stage4_aggregated


def main():
    xlsx_path = ROOT / "nico_in" / "eff_all.xlsx"
    if not xlsx_path.exists():
        print(f"ERROR: {xlsx_path} not found")
        sys.exit(1)

    print(f"Reading {xlsx_path}...")
    df = pd.read_excel(xlsx_path, engine="openpyxl")
    print(f"  Raw rows: {len(df)}, columns: {list(df.columns)}")

    # Convert Date column — may be Excel serial numbers or datetime objects
    if df["Date"].dtype == "float64" or df["Date"].dtype == "int64":
        # Excel serial number: days since 1899-12-30
        df["Date"] = pd.to_timedelta(df["Date"], unit="D") + pd.Timestamp("1899-12-30")
    df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")

    # Ensure EmployeeID is string
    df["EmployeeID"] = df["EmployeeID"].astype(str)

    # Ensure string columns are actually strings (Excel may have mixed types)
    for col in ["Gusto Name", "MT Name", "Team"]:
        df[col] = df[col].astype(str).replace("nan", "")

    # Validate expected columns
    expected = [
        "Date", "EmployeeID", "Gusto Name", "MT Name", "Team",
        "Training Plan", "Work Hours", "Cases_Worked_On",
        "Tasks_Completed", "Tasks_Duration_Hours", "Efficiency",
    ]
    missing = [c for c in expected if c not in df.columns]
    if missing:
        print(f"ERROR: Missing columns: {missing}")
        sys.exit(1)

    # Sort: Date desc, EmployeeID asc
    df = df.sort_values(["Date", "EmployeeID"], ascending=[False, True]).reset_index(drop=True)

    # Save daily parquet
    print(f"Saving daily.parquet ({len(df)} rows)...")
    save_daily(df)

    # Build aggregated view
    print("Building aggregated efficiency (Stage 4)...")
    agg = stage4_aggregated(df)
    print(f"Saving aggregated.parquet ({len(agg)} rows)...")
    save_aggregated(agg)

    # Summary
    dates = sorted(df["Date"].unique())
    print(f"\nDone!")
    print(f"  Total rows: {len(df)}")
    print(f"  Date range: {dates[0]} to {dates[-1]} ({len(dates)} days)")
    print(f"  Unique employees: {df['MT Name'].nunique()}")
    print(f"  Aggregated rows: {len(agg)}")


if __name__ == "__main__":
    main()
