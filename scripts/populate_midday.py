"""
Populate noon and 3pm parquet files so the midday tabs are never blank.
Run from project root: python scripts/populate_midday.py
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dashboard.data.efficiency_processing import process_midday_snapshot
from dashboard.data.efficiency_store import save_midday


def main():
    for window in ("noon", "3pm"):
        print(f"Processing {window} snapshot...")
        try:
            df = process_midday_snapshot(window)
            if df.empty:
                print(f"  No data returned for {window}")
            else:
                save_midday(window, df)
                print(f"  Saved {len(df)} rows for {window}")
        except Exception as e:
            print(f"  Error processing {window}: {e}")

    print("Done!")


if __name__ == "__main__":
    main()
