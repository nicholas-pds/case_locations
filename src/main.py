# src/main.py
from pathlib import Path
from .db_handler import execute_sql_to_dataframe
from .sheets_handler import SheetsHandler

def main():
    """Main function to orchestrate the daily process."""
    
    # --- Dynamically determine the SQL file path ---
    BASE_DIR = Path(__file__).parent
    SQL_FILE_PATH = BASE_DIR.parent / "sql_query" / "case_locs_1.sql"
    
    # --- Configuration ---
    SHEET_NAME = "Report"  # The tab name in your Google Sheet
    # ---------------------------------------------------
    
    print(f"Attempting to load SQL file from: {SQL_FILE_PATH}")

    # Step 1: Connect to DB, run query, and get DataFrame
    print("Starting database operation...")
    
    try:
        data_df = execute_sql_to_dataframe(str(SQL_FILE_PATH))
    except FileNotFoundError:
        print(f"🚨 ERROR: SQL file not found at the expected path: {SQL_FILE_PATH}")
        return
    except Exception as e:
        print(f"🚨 ERROR during database operation: {e}")
        return

    if not data_df.empty:
        print("\n--- DataFrame Head ---")
        print(data_df.head())
        print(f"\nTotal rows retrieved: {len(data_df)}")
        
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
                print("✅ Successfully uploaded data to Google Sheets!")
            else:
                print("⚠️ Upload to Google Sheets failed.")
                
        except Exception as e:
            print(f"🚨 ERROR with Google Sheets operation: {e}")
        
    else:
        print("Data extraction failed or returned an empty result. Stopping execution.")
        
if __name__ == "__main__":
    # Reminder: Run this from the project root using 'uv run python -m src.main'
    main()