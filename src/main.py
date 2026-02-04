# src/main.py
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
from .db_handler import execute_sql_to_dataframe
from .sheets_handler import SheetsHandler
from .holidays import previous_business_day
from .date_parser import process_dataframe, sort_by_follow_up_date

def main():
    """Main function to orchestrate the daily process."""
    
    # --- Dynamically determine the SQL file paths ---
    BASE_DIR = Path(__file__).parent
    SQL_FILE_PATH_1 = BASE_DIR.parent / "sql_query" / "case_locs_1.sql"
    SQL_FILE_PATH_2 = BASE_DIR.parent / "sql_query" / "cases_Prod_and_Invoiced.sql"
    SQL_FILE_PATH_3 = BASE_DIR.parent / "sql_query" / "case_locs_airway_1.sql"
    SQL_FILE_PATH_4 = BASE_DIR.parent / "sql_query" / "airway_hold_status_1.sql"
    
    # --- Configuration ---
    # Query 1 Configuration
    SHEET_NAME_1 = "Report"
    SHIP_DATE_COLUMN_1 = "Ship Date"
    CATEGORY_COLUMN = "Category"
    DAYS_LOOKBACK_1 = 6
    START_CELL_1 = "C5"  # Write to C6, no headers
    INCLUDE_HEADERS_1 = True
    
    # Query 2 Configuration
    SHEET_NAME_2 = "Report 3"
    SHIP_DATE_COLUMN_2 = "ShipDate"
    DAYS_RANGE_2 = 9  # Previous business day + 7 days = 8 total days
    START_CELL_2 = "A1"  # Write to A1, with headers
    INCLUDE_HEADERS_2 = True

    # Query 3 Configuration
    SHEET_NAME_3 = "Report 4"
    START_CELL_3 = "A1"
    INCLUDE_HEADERS_3 = True

    # Query 4 Configuration
    SHEET_NAME_4 = "Report 5"
    START_CELL_4 = "A1"
    INCLUDE_HEADERS_4 = True
    # ---------------------------------------------------
    
    # Initialize the sheets handler once (reads credentials from .env)
    try:
        sheets = SheetsHandler()
    except Exception as e:
        print(f"ERROR initializing Google Sheets handler: {e}")
        return
    
    # ========================================================================
    # QUERY 1: case_locs_1.sql → "Report" sheet (C6, no headers)
    # ========================================================================
    print("=" * 70)
    print("PROCESSING QUERY 1: case_locs_1.sql")
    print("=" * 70)
    print(f"Attempting to load SQL file from: {SQL_FILE_PATH_1}")
    
    try:
        data_df_1 = execute_sql_to_dataframe(str(SQL_FILE_PATH_1))
    except FileNotFoundError:
        print(f"ERROR: SQL file not found at: {SQL_FILE_PATH_1}")
        data_df_1 = pd.DataFrame()
    except Exception as e:
        print(f"ERROR during database operation: {e}")
        data_df_1 = pd.DataFrame()
    
    if not data_df_1.empty:
        print("\n--- DataFrame Head (Before Filter) ---")
        print(data_df_1.head())
        print(f"Total rows retrieved: {len(data_df_1)}")
        
        # Apply dynamic date range filter
        print("\n--- Applying Date Range Filter ---")
        try:
            cutoff_date = datetime.now() - timedelta(days=DAYS_LOOKBACK_1)
            print(f"Filtering for dates after: {cutoff_date.date()}")
            
            data_df_1[SHIP_DATE_COLUMN_1] = pd.to_datetime(data_df_1[SHIP_DATE_COLUMN_1])
            data_df_1 = data_df_1[data_df_1[SHIP_DATE_COLUMN_1] > cutoff_date]
            
            print(f"Filter applied. Rows after filter: {len(data_df_1)}")
            print("\n--- DataFrame Head (After Filter) ---")
            print(data_df_1.head())
            
        except KeyError:
            print(f"ERROR: Column '{SHIP_DATE_COLUMN_1}' not found in DataFrame")
            print(f"Available columns: {list(data_df_1.columns)}")
        except Exception as e:
            print(f"ERROR during date filtering: {e}")
        else:
            # Convert Ship Date back to date only
            print("\n--- Converting Ship Date to date only ---")
            data_df_1[SHIP_DATE_COLUMN_1] = data_df_1[SHIP_DATE_COLUMN_1].dt.date
            
            # Replace 'Airway' with 'MARPE' in Category Column
            print("\n--- Replacing 'Airway' with 'MARPE' in Category Column ---")
            if CATEGORY_COLUMN in data_df_1.columns:
                before_count = (data_df_1[CATEGORY_COLUMN] == 'Airway').sum()
                data_df_1[CATEGORY_COLUMN] = data_df_1[CATEGORY_COLUMN].replace('Airway', 'MARPE')
                print(f"Replaced {before_count} instance(s) of 'Airway' → 'MARPE'")
                
                # Fill missing values in Category Column with 'Other'
                print("\n--- Filling Missing Values in Category Column ---")
                count_no_category = data_df_1[CATEGORY_COLUMN].isna().sum()
                data_df_1[CATEGORY_COLUMN] = data_df_1[CATEGORY_COLUMN].fillna('Other')
                print(f"Filled {count_no_category} missing category(ies) with 'Other'")

                # Filter out MARPE rows with specific Last Location values (planning/consultation stages)
                print("\n--- Filtering MARPE Rows in Planning Stages ---")
                MARPE_EXCLUDED_LOCATIONS = [
                    'New Cases',
                    'New Cases How to Proceed',
                    'New Cases Waiting For Scans',
                    'Email Plan Case',
                    'Email Follow Up',
                    'Zoom Set Up',
                    'Zoom Consult',
                    'Zoom Export Needed',
                    'Zoom Waiting Approval',
                    'Airway Zoom Plan',
                    'Airway Email Approval',
                    'Airway Planning',
                    'Airway Email Plan',
                    'Airway Zoom Approval'
                ]
                before_filter_count = len(data_df_1)
                mask = ~((data_df_1[CATEGORY_COLUMN] == 'MARPE') &
                         (data_df_1['Last Location'].isin(MARPE_EXCLUDED_LOCATIONS)))
                data_df_1 = data_df_1[mask]
                removed_count = before_filter_count - len(data_df_1)
                print(f"Removed {removed_count} MARPE row(s) in planning stages. Rows remaining: {len(data_df_1)}")
            else:
                print(f"WARNING: Column '{CATEGORY_COLUMN}' not found. Skipping replacement.")
                print(f"Available columns: {list(data_df_1.columns)}")
            
            # Upload to Google Sheets at C6 without headers
            print(f"\n--- Uploading Query 1 to Google Sheets ('{SHEET_NAME_1}' at {START_CELL_1}, no headers) ---")
            try:
                success = sheets.write_dataframe_to_sheet(
                    df=data_df_1,
                    sheet_name=SHEET_NAME_1,
                    clear_sheet=True,
                    start_cell=START_CELL_1,
                    include_headers=INCLUDE_HEADERS_1
                )
                
                if success:
                    print(f"✓ Successfully uploaded data to '{SHEET_NAME_1}' sheet at {START_CELL_1}!")
                else:
                    print(f"✗ Upload to '{SHEET_NAME_1}' sheet failed.")
                    
            except Exception as e:
                print(f"ERROR with Google Sheets operation: {e}")
    else:
        print("Query 1 data extraction failed or returned empty result.")
    
    # ========================================================================
    # QUERY 2: cases_Prod_and_Invoiced.sql → "Report 3" sheet (A1, with headers)
    # ========================================================================
    print("\n" + "=" * 70)
    print("PROCESSING QUERY 2: cases_Prod_and_Invoiced.sql")
    print("=" * 70)
    print(f"Attempting to load SQL file from: {SQL_FILE_PATH_2}")
    
    query_2_success = False
    try:
        data_df_2 = execute_sql_to_dataframe(str(SQL_FILE_PATH_2))
        query_2_success = True
    except FileNotFoundError:
        print(f"ERROR: SQL file not found at: {SQL_FILE_PATH_2}")
        data_df_2 = pd.DataFrame()
    except Exception as e:
        print(f"ERROR during database operation: {e}")
        data_df_2 = pd.DataFrame()

    if query_2_success:
        print(f"Total rows retrieved: {len(data_df_2)}")

        if not data_df_2.empty:
            print("\n--- DataFrame Head (Before Filter) ---")
            print(data_df_2.head())

            # Apply business day date range filter
            print("\n--- Applying Business Day Date Range Filter ---")
            try:
                # Get previous business day (excluding weekends and holidays)
                prev_biz_day = previous_business_day()
                start_date = prev_biz_day
                end_date = prev_biz_day + timedelta(days=DAYS_RANGE_2)

                print(f"Previous business day: {start_date}")
                print(f"Date range: {start_date} to {end_date} (inclusive)")

                # Convert ShipDate column to datetime
                data_df_2[SHIP_DATE_COLUMN_2] = pd.to_datetime(data_df_2[SHIP_DATE_COLUMN_2])

                # Filter: start_date <= ShipDate <= end_date
                data_df_2 = data_df_2[
                    (data_df_2[SHIP_DATE_COLUMN_2].dt.date >= start_date) &
                    (data_df_2[SHIP_DATE_COLUMN_2].dt.date <= end_date)
                ]

                print(f"Filter applied. Rows after filter: {len(data_df_2)}")
                if not data_df_2.empty:
                    print("\n--- DataFrame Head (After Filter) ---")
                    print(data_df_2.head())

                # Convert ShipDate back to date only
                print("\n--- Converting ShipDate to date only ---")
                data_df_2[SHIP_DATE_COLUMN_2] = data_df_2[SHIP_DATE_COLUMN_2].dt.date

            except KeyError:
                print(f"ERROR: Column '{SHIP_DATE_COLUMN_2}' not found in DataFrame")
                print(f"Available columns: {list(data_df_2.columns)}")
            except Exception as e:
                print(f"ERROR during date filtering: {e}")
        else:
            print("Query returned 0 rows - will write headers only")

        # Upload to Google Sheets at A1 with headers (even if empty, writes headers)
        print(f"\n--- Uploading Query 2 to Google Sheets ('{SHEET_NAME_2}' at {START_CELL_2}, with headers) ---")
        try:
            success = sheets.write_dataframe_to_sheet(
                df=data_df_2,
                sheet_name=SHEET_NAME_2,
                clear_sheet=True,
                start_cell=START_CELL_2,
                include_headers=INCLUDE_HEADERS_2
            )

            if success:
                print(f"✓ Successfully uploaded data to '{SHEET_NAME_2}' sheet at {START_CELL_2}!")
            else:
                print(f"✗ Upload to '{SHEET_NAME_2}' sheet failed.")

        except Exception as e:
            print(f"ERROR with Google Sheets operation: {e}")
    else:
        print("Query 2 failed to execute.")

    # ========================================================================
    # QUERY 3: case_locs_airway_1.sql → "Report 4" sheet (A1, with headers)
    # ========================================================================
    print("\n" + "=" * 70)
    print("PROCESSING QUERY 3: case_locs_airway_1.sql")
    print("=" * 70)
    print(f"Attempting to load SQL file from: {SQL_FILE_PATH_3}")

    query_3_success = False
    try:
        data_df_3 = execute_sql_to_dataframe(str(SQL_FILE_PATH_3))
        query_3_success = True
    except FileNotFoundError:
        print(f"ERROR: SQL file not found at: {SQL_FILE_PATH_3}")
        data_df_3 = pd.DataFrame()
    except Exception as e:
        print(f"ERROR during database operation: {e}")
        data_df_3 = pd.DataFrame()

    if query_3_success:
        print(f"Total rows retrieved: {len(data_df_3)}")

        if not data_df_3.empty:
            print("\n--- DataFrame Head ---")
            print(data_df_3.head())
        else:
            print("Query returned 0 rows - will write headers only")

        # Upload to Google Sheets at A1 with headers (even if empty, writes headers)
        print(f"\n--- Uploading Query 3 to Google Sheets ('{SHEET_NAME_3}' at {START_CELL_3}, with headers) ---")
        try:
            success = sheets.write_dataframe_to_sheet(
                df=data_df_3,
                sheet_name=SHEET_NAME_3,
                clear_sheet=True,
                start_cell=START_CELL_3,
                include_headers=INCLUDE_HEADERS_3
            )

            if success:
                print(f"✓ Successfully uploaded data to '{SHEET_NAME_3}' sheet at {START_CELL_3}!")
            else:
                print(f"✗ Upload to '{SHEET_NAME_3}' sheet failed.")

        except Exception as e:
            print(f"ERROR with Google Sheets operation: {e}")
    else:
        print("Query 3 failed to execute.")

    # ========================================================================
    # QUERY 4: airway_hold_status_1.sql → "Report 5" sheet (A1, with headers)
    # ========================================================================
    print("\n" + "=" * 70)
    print("PROCESSING QUERY 4: airway_hold_status_1.sql")
    print("=" * 70)
    print(f"Attempting to load SQL file from: {SQL_FILE_PATH_4}")

    query_4_success = False
    try:
        data_df_4 = execute_sql_to_dataframe(str(SQL_FILE_PATH_4))
        query_4_success = True
    except FileNotFoundError:
        print(f"ERROR: SQL file not found at: {SQL_FILE_PATH_4}")
        data_df_4 = pd.DataFrame()
    except Exception as e:
        print(f"ERROR during database operation: {e}")
        data_df_4 = pd.DataFrame()

    if query_4_success:
        print(f"Total rows retrieved: {len(data_df_4)}")

        if not data_df_4.empty:
            print("\n--- DataFrame Head (Before Processing) ---")
            print(data_df_4.head())

            # Rename 'TYPE' column to 'FollowUpType'
            print("\n--- Renaming 'TYPE' column to 'FollowUpType' ---")
            if 'TYPE' in data_df_4.columns:
                data_df_4 = data_df_4.rename(columns={'TYPE': 'FollowUpType'})
                print("Column renamed successfully")
            else:
                print("WARNING: 'TYPE' column not found in DataFrame")

            # Add FollowUpDate column by parsing HoldReason
            print("\n--- Adding 'FollowUpDate' column (parsed from HoldReason) ---")
            data_df_4 = process_dataframe(data_df_4)
            fu_date_count = data_df_4['FollowUpDate'].notna().sum()
            print(f"Parsed {fu_date_count} follow-up date(s) from HoldReason")

            # Sort by FollowUpType and FollowUpDate
            print("\n--- Sorting by FollowUpType and FollowUpDate ---")
            data_df_4 = sort_by_follow_up_date(data_df_4)
            print("Sorted: FollowUpType cases first, ordered by FollowUpDate")

            print("\n--- DataFrame Head (After Processing) ---")
            print(data_df_4.head())
        else:
            print("Query returned 0 rows - will write headers only")

        # Upload to Google Sheets at A1 with headers
        print(f"\n--- Uploading Query 4 to Google Sheets ('{SHEET_NAME_4}' at {START_CELL_4}, with headers) ---")
        try:
            success = sheets.write_dataframe_to_sheet(
                df=data_df_4,
                sheet_name=SHEET_NAME_4,
                clear_sheet=True,
                start_cell=START_CELL_4,
                include_headers=INCLUDE_HEADERS_4
            )

            if success:
                print(f"✓ Successfully uploaded data to '{SHEET_NAME_4}' sheet at {START_CELL_4}!")
            else:
                print(f"✗ Upload to '{SHEET_NAME_4}' sheet failed.")

        except Exception as e:
            print(f"ERROR with Google Sheets operation: {e}")
    else:
        print("Query 4 failed to execute.")

    print("\n" + "=" * 70)
    print("PROCESSING COMPLETE")
    print("=" * 70)

if __name__ == "__main__":
    # Reminder: Run this from the project root using 'uv run python -m src.main'
    main()