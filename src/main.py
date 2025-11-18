# src/main.py
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
from .db_handler import execute_sql_to_dataframe
from .sheets_handler import SheetsHandler

def main():
    """Main function to orchestrate the daily process."""
    
    # --- Dynamically determine the SQL file path ---
    BASE_DIR = Path(__file__).parent
    SQL_FILE_PATH = BASE_DIR.parent / "sql_query" / "case_locs_1.sql"
    
    # --- Configuration ---
    SHEET_NAME = "Report"  # The tab name in your Google Sheet
    SHIP_DATE_COLUMN = "Ship Date"  # The column name to filter on
    CATEGORY_COLUMN = "Category"    # The column name for category replacement
    # ---------------------------------------------------
    
    print(f"Attempting to load SQL file from: {SQL_FILE_PATH}")

    # Step 1: Connect to DB, run query, and get DataFrame
    print("Starting database operation...")
    
    try:
        data_df = execute_sql_to_dataframe(str(SQL_FILE_PATH))
    except FileNotFoundError:
        print(f"ERROR: SQL file not found at the expected path: {SQL_FILE_PATH}")
        return
    except Exception as e:
        print(f"ERROR during database operation: {e}")
        return

    if not data_df.empty:
        print("\n--- DataFrame Head (Before Filter) ---")
        print(data_df.head())
        print(f"Total rows retrieved: {len(data_df)}")
        
        # Step 1.5: Apply dynamic date range filter
        print("\n--- Applying Date Range Filter ---")
        try:
            # Calculate the cutoff date (4 days ago)
            cutoff_date = datetime.now() - timedelta(days=4)
            print(f"Filtering for dates after: {cutoff_date.date()}")
            
            # Convert Ship Date column to datetime if not already
            data_df[SHIP_DATE_COLUMN] = pd.to_datetime(data_df[SHIP_DATE_COLUMN])
            
            # Filter: Ship Date > 4 days ago
            data_df = data_df[data_df[SHIP_DATE_COLUMN] > cutoff_date]
            
            print(f"Filter applied. Rows after filter: {len(data_df)}")
            print("\n--- DataFrame Head (After Filter) ---")
            print(data_df.head())
            
        except KeyError:
            print(f"ERROR: Column '{SHIP_DATE_COLUMN}' not found in DataFrame")
            print(f"Available columns: {list(data_df.columns)}")
            return
        except Exception as e:
            print(f"ERROR during date filtering: {e}")
            return
        
        # Step 1.6: Convert Ship Date back to date only (remove time component)
        print("\n--- Converting Ship Date to date only ---")
        data_df[SHIP_DATE_COLUMN] = data_df[SHIP_DATE_COLUMN].dt.date

        # Step 1.7: In the Category Column find and replace 'Airway' with 'MARPE'
        print("\n--- Replacing 'Airway' with 'MARPE' in Category Column ---")
        if CATEGORY_COLUMN in data_df.columns:
            before_count = (data_df[CATEGORY_COLUMN] == 'Airway').sum()
            data_df[CATEGORY_COLUMN] = data_df[CATEGORY_COLUMN].replace('Airway', 'MARPE')
            after_count = (data_df[CATEGORY_COLUMN] == 'MARPE').sum()
            print(f"Replaced {before_count} instance(s) of 'Airway' → 'MARPE'")
            # Step 1.8: Fill missing values in Category Column with 'Other'
            print("\n--- Filling Missing Values in Category Column ---")
            count_no_category = data_df[CATEGORY_COLUMN].isna().sum()
            data_df[CATEGORY_COLUMN] = data_df[CATEGORY_COLUMN].fillna('Other')
            print(f"Filled {count_no_category} missing category(ies) with 'Other'")
        else:
            print(f"WARNING: Column '{CATEGORY_COLUMN}' not found. Skipping replacement.")
            print(f"Available columns: {list(data_df.columns)}")
        
        # Step 2: Upload to Google Sheets
        print("\n--- Uploading to Google Sheets ---")
        
        try:
            # Initialize the sheets handler (reads credentials from .env)
            sheets = SheetsHandler()
            
            # Upload DataFrame to Google Sheets
            success = sheets.write_dataframe_to_sheet(
                df=data_df,
                sheet_name=SHEET_NAME,
                clear_sheet=True  # Clear existing data before writing
            )
            
            if success:
                print("Successfully uploaded data to Google Sheets!")
            else:
                print("Upload to Google Sheets failed.")
                
        except Exception as e:
            print(f"ERROR with Google Sheets operation: {e}")
        
    else:
        print("Data extraction failed or returned an empty result. Stopping execution.")
        
if __name__ == "__main__":
    # Reminder: Run this from the project root using 'uv run python -m src.main'
    main()