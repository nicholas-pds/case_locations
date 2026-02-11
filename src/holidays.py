# src/holidays.py
import csv
import logging
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Set

logger = logging.getLogger(__name__)

# Path to the company holidays CSV file (project root)
_CSV_PATH = Path(__file__).parent.parent / "company_holidays.csv"


def _load_holidays_from_csv() -> Set[date]:
    """Load holidays from the CSV file. Returns empty set if file not found."""
    holidays = set()
    if not _CSV_PATH.exists():
        return holidays
    try:
        with open(_CSV_PATH, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    holidays.add(date.fromisoformat(row['date'].strip()))
                except (ValueError, KeyError):
                    continue
    except Exception as e:
        logger.warning(f"Failed to read holidays CSV: {e}")
        return set()
    return holidays


def get_company_holidays(year: int) -> Set[date]:
    """
    Returns a set of company holidays (US federal + extra company days) for a given year.
    """
    holidays = set()

    # New Year's Day
    holidays.add(date(year, 1, 1))

    # Day after New Year's Day (company-specific)
    holidays.add(date(year, 1, 2))

    # Memorial Day – last Monday in May
    may_31 = date(year, 5, 31)
    days_back_to_monday = (may_31.weekday() + 1) % 7
    memorial_day = may_31 - timedelta(days=days_back_to_monday)
    holidays.add(memorial_day)

    # Independence Day
    holidays.add(date(year, 7, 4))

    # Labor Day – first Monday in September
    sep_1 = datetime(year, 9, 1)
    days_to_monday = (7 - sep_1.weekday()) % 7
    labor_day = sep_1 + timedelta(days=days_to_monday)
    holidays.add(labor_day.date())

    # Veterans Day
    holidays.add(date(year, 11, 11))

    # Thanksgiving – 4th Thursday in November
    nov_1 = datetime(year, 11, 1)
    days_to_thursday = (3 - nov_1.weekday()) % 7
    first_thursday = nov_1 + timedelta(days=days_to_thursday)
    thanksgiving = first_thursday + timedelta(days=21)  # 4th Thursday
    holidays.add(thanksgiving.date())

    # Day after Thanksgiving (company-specific – usually Nov 28 or 29, but calculated properly)
    day_after_thanksgiving = thanksgiving + timedelta(days=1)
    holidays.add(day_after_thanksgiving.date())

    # Christmas Eve (optional – many companies close early/close)
    # holidays.add(date(year, 12, 24))

    # Christmas Day
    holidays.add(date(year, 12, 25))

    # Day after Christmas (company-specific)
    holidays.add(date(year, 12, 26))

    return holidays


def get_all_company_holidays(start_year: int = 2025, end_year: int = None) -> Set[date]:
    """
    Returns company holidays. Loads from company_holidays.csv if available,
    otherwise falls back to the computed/hardcoded calendar.
    """
    csv_holidays = _load_holidays_from_csv()
    if csv_holidays:
        return csv_holidays

    # Fallback: computed holidays
    if end_year is None:
        end_year = date.today().year + 2
    all_holidays: Set[date] = set()
    for y in range(start_year, end_year + 1):
        all_holidays.update(get_company_holidays(y))
    return all_holidays


def previous_business_day(reference_date: date = None, holidays: Set[date] = None) -> date:
    """
    Returns the most recent business day BEFORE reference_date (default: today)
    that is NOT a weekend and NOT in the company holidays list.
    """
    if reference_date is None:
        reference_date = date.today()

    if holidays is None:
        holidays = get_all_company_holidays()

    candidate = reference_date - timedelta(days=1)

    while candidate.weekday() >= 5 or candidate in holidays:  # Saturday=5, Sunday=6
        candidate -= timedelta(days=1)

    return candidate

def next_x_business_days(reference_date: date = None,x_days_ahead: int = None, holidays: Set[date] = None) -> date:
    """
    Returns the date exactly 5 business days AFTER reference_date (default: today)
    that is NOT a weekend and NOT in the company holidays list.
    """
    if reference_date is None:
        reference_date = date.today()

    if holidays is None:
        holidays = get_all_company_holidays()

    if x_days_ahead is None:
        x_days_ahead = 5
    
    # Start checking from the day AFTER the reference_date
    candidate = reference_date + timedelta(days=1)
    
    # Initialize the business day counter
    business_days_found = 0

    # We loop until we have found x valid business days
    while business_days_found < x_days_ahead:
        
        # Check if the current candidate date is a weekend (Saturday=5, Sunday=6) or a holiday
        if candidate.weekday() < 5 and candidate not in holidays:
            # If it is a valid business day, increment the counter
            business_days_found += 1
        
        # Move to the next day, regardless of whether the current day was a business day or not
        # (This ensures we skip weekends and holidays)
        candidate += timedelta(days=1)
        
    # The loop exits when business_days_found is X. 
    # Since the last step of the loop (candidate += timedelta(days=1)) 
    # executes *after* finding the x_th day, we must subtract one day from the candidate.
    return candidate - timedelta(days=1)

# Optional: convenience alias
prev_business_day = previous_business_day


if __name__ == "__main__":
    # Quick test when running the file directly
    today = date.today()
    prev = previous_business_day()
    print(f"Today: {today}")
    print(f"Previous business day (excluding holidays): {prev}")
    print(f"Is {prev} a holiday? {prev in get_all_company_holidays()}")