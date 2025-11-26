# src/workload_email_handler.py
import os
from pathlib import Path
from datetime import date, timedelta
import pandas as pd

# --- NEW IMPORTS ---
from .db_handler import execute_sql_to_dataframe
from .holidays import previous_business_day, next_x_business_days
from .email_handler import email_dataframes # Assuming email_handler.py is in the same source directory
# -------------------

# --- NEW GLOBAL CONSTANT ---
#RECIPIENTS = os.getenv("WORKLOAD_EMAIL_RECIPIENTS", "partners@example.com").split(',')
RECIPIENTS = ["nick@partnersdentalstudio.com"]
#RECIPIENTS = ["nick@partnersdentalstudio.com", "sarah@partnersdentalstudio.com"]
# ---------------------------


def main():
    """Load workload_pivot.sql, generate a clean pivot table, and send it via email."""
    
    # Check if we have recipients before doing heavy work
    if not RECIPIENTS or RECIPIENTS == ['']:
        print("ERROR: No email recipients defined. Check WORKLOAD_EMAIL_RECIPIENTS variable.")
        return
    
    # --- SQL file path ---
    BASE_DIR = Path(__file__).parent
    SQL_FILE_PATH = BASE_DIR.parent / "sql_query" / "workload_pivot.sql"
    
    print(f"Loading SQL query from: {SQL_FILE_PATH.resolve()}")
    
    # --- Execute query ---
    print("Executing query against database...")
    try:
        # Assuming execute_sql_to_dataframe returns a DataFrame (raw_df)
        raw_df = execute_sql_to_dataframe(str(SQL_FILE_PATH))
    except FileNotFoundError:
        print(f"ERROR: SQL file not found at {SQL_FILE_PATH}")
        return
    except Exception as e:
        print(f"ERROR during query execution: {e}")
        return

    if raw_df.empty:
        print("Query returned no data. Skipping email.")
        return

    print(f"Raw data retrieved: {len(raw_df):,} rows")
    print(f"Columns: {list(raw_df.columns)}")
    print("\nFirst 5 rows of raw data:")
    print(raw_df.head())

    # --- Data cleanup ---
    # dynamic date range
    start_date = previous_business_day()
    # Fixed the argument name to match the previous function definition
    end_date = next_x_business_days(reference_date=start_date, x_days_ahead=7)
    
    df = raw_df[(raw_df['ShipDate'] >= start_date) & (raw_df['ShipDate'] < end_date)].copy()
    
    print(f"\nData filtered for ShipDate from {start_date} to {end_date} (7 days)")
    
    if df.empty:
        print("Data is empty after date filtering. Skipping email.")
        return

    # Ensure ShipDate is date only
    if 'ShipDate' in df.columns:
        df['ShipDate'] = pd.to_datetime(df['ShipDate'], errors='coerce').dt.date

    # --- Build pivot table (Summary DataFrame) ---
    print("\n" + "="*80)
    print("DAILY WORKLOAD PIVOT: Cases by Category & Ship Date")
    print("="*80)

    pivot = (
        df.pivot_table(
            index='Category',
            columns='ShipDate',
            values='CaseCount',
            aggfunc='sum',
            fill_value=0,
            margins=True,
            margins_name='TOTAL'
        )
        # Sort columns: most recent dates first (excluding the 'TOTAL' column)
        .sort_index(axis=1, key=lambda x: pd.to_datetime(x, errors='coerce'), ascending=True)
    )

    # Convert all numbers to integers for clean display
    pivot = pivot.astype(int)
    
    # Pretty print
    print(pivot.to_string())

    print("\n" + "="*80)
    print("Pivot table generated successfully.")
    print(f"Total cases in period: {pivot.loc['TOTAL', 'TOTAL']:,}")
    print("="*80)
    
    # --- SEND EMAIL ---
    try:
        print("\nAttempting to send workload summary email...")
        email_dataframes(
            summary_df=pivot,               # The main, summarized table
            #raw_df=raw_df,                  # The raw results table
            recipients=RECIPIENTS,          # The list of emails to send to
            subject=f"Workload Summary: {start_date} to {end_date}",
            # Optional: Pass custom SMTP details or rely on environment variables
            # smtp_server="...",
        )
        print("Email sending initiated.")
    except ValueError as e:
        print(f"\nEMAIL ERROR: Failed to send email due to missing credentials: {e}")
    except Exception as e:
        print(f"\nEMAIL ERROR: An unexpected error occurred while sending the email: {e}")


if __name__ == "__main__":
    # Run with: uv run python -m src.workload_email_handler
    main()