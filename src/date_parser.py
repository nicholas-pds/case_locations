"""
date_parser.py - AFU/ZFU/EFU date extraction and resolution

Handles parsing of follow-up dates from HoldReason text with intelligent
year assignment based on proximity to current date.
"""

import re
from datetime import date
from typing import Optional
import pandas as pd


# Pattern to find AFU/ZFU/EFU markers followed by a date
# Matches: (AFU) 12/15, (AFU)12/15, AFU 12/15, etc.
MARKER_DATE_PATTERN = re.compile(
    r'\(?(AFU|ZFU|EFU)\)?\s*(\d{1,2})/(\d{1,2})',
    re.IGNORECASE
)


def extract_follow_up_date(
    hold_reason: str,
    reference_date: Optional[date] = None
) -> Optional[date]:
    """
    Extract and resolve a follow-up date from HoldReason text.

    If multiple dates are found, returns the most recent one.

    Args:
        hold_reason: The raw HoldReason text (e.g., "(AFU) 12/15 waiting...")
        reference_date: Date to use as "today" (defaults to date.today())

    Returns:
        Resolved date with year, or None if no valid date found.
    """
    if reference_date is None:
        reference_date = date.today()

    if not hold_reason or pd.isna(hold_reason):
        return None

    # Find ALL marker+date patterns in the text
    matches = MARKER_DATE_PATTERN.findall(hold_reason)

    if not matches:
        return None

    # Resolve year for each date and collect valid ones
    resolved_dates = []
    for match in matches:
        # match is (type, month, day)
        try:
            month = int(match[1])
            day = int(match[2])
        except (ValueError, IndexError):
            continue

        # Validate month and day ranges
        if not (1 <= month <= 12) or not (1 <= day <= 31):
            continue

        resolved = _resolve_year(month, day, reference_date)
        if resolved:
            resolved_dates.append(resolved)

    if not resolved_dates:
        return None

    # Return the most recent (latest) date
    return max(resolved_dates)


def _resolve_year(
    month: int,
    day: int,
    reference_date: date
) -> Optional[date]:
    """
    Resolve the correct year for a MM/DD date.

    Strategy:
        - Consider PREVIOUS, CURRENT, and NEXT year as candidates
        - Prefer past dates (with a bias) since follow-ups typically
          refer to past events
        - Choose the date closest to reference, with past-bias

    Args:
        month: Month (1-12)
        day: Day of month (1-31)
        reference_date: The reference date for proximity calculation

    Returns:
        The resolved date with year, or None if invalid.
    """
    current_year = reference_date.year
    candidates = []

    # Generate candidate dates for prev, current, next year
    for year_offset in [-1, 0, 1]:
        year = current_year + year_offset
        try:
            candidate = date(year, month, day)
            candidates.append(candidate)
        except ValueError:
            # Invalid date (e.g., Feb 30) - skip this candidate
            continue

    if not candidates:
        return None

    # Score each candidate - lower score is better
    # Past dates get a bonus (negative adjustment)
    PAST_BIAS_DAYS = 30  # Past dates get 30-day "bonus" in scoring
    MAX_FUTURE_DAYS = 180  # Don't consider dates more than 6 months ahead
    MAX_PAST_DAYS = 365  # Don't consider dates more than 1 year ago

    def score_candidate(candidate: date) -> float:
        days_diff = (candidate - reference_date).days

        # Filter out dates too far in future or past
        if days_diff > MAX_FUTURE_DAYS:
            return float('inf')
        if days_diff < -MAX_PAST_DAYS:
            return float('inf')

        # Absolute distance, but subtract bias for past dates
        if days_diff <= 0:  # Past or today
            return abs(days_diff) - PAST_BIAS_DAYS
        else:  # Future
            return days_diff

    # Find best candidate (lowest score)
    scored = [(c, score_candidate(c)) for c in candidates]
    valid = [(c, s) for c, s in scored if s != float('inf')]

    if not valid:
        return None

    best = min(valid, key=lambda x: x[1])
    return best[0]


def process_dataframe(
    df: pd.DataFrame,
    hold_reason_column: str = 'HoldReason',
    output_column: str = 'FollowUpDate',
    reference_date: Optional[date] = None
) -> pd.DataFrame:
    """
    Process a DataFrame to add/update the FollowUpDate column.

    Args:
        df: Input DataFrame with HoldReason column
        hold_reason_column: Name of the column containing hold reasons
        output_column: Name of the column to store parsed dates
        reference_date: Reference date for year resolution (default: today)

    Returns:
        DataFrame with new/updated FollowUpDate column
    """
    df = df.copy()

    df[output_column] = df[hold_reason_column].apply(
        lambda x: extract_follow_up_date(x, reference_date)
    )

    return df


def sort_by_follow_up_date(
    df: pd.DataFrame,
    type_column: str = 'FollowUpType',
    date_column: str = 'FollowUpDate'
) -> pd.DataFrame:
    """
    Sort DataFrame with AFU/ZFU/EFU cases first, ordered by FollowUpDate.

    Replicates the original SQL ordering logic:
    - Cases with FollowUpType (AFU/ZFU/EFU) come first
    - Within those, sort by FollowUpDate ascending (oldest first)
    - Cases without FollowUpType come last

    Args:
        df: DataFrame to sort
        type_column: Column containing AFU/ZFU/EFU type
        date_column: Column containing parsed follow-up dates

    Returns:
        Sorted DataFrame
    """
    df = df.copy()

    # Create sort key: 0 for records with FollowUpType, 1 for others
    df['_has_type'] = df[type_column].notna().apply(lambda x: 0 if x else 1)

    # Sort: first by _has_type, then by FollowUpDate
    df = df.sort_values(
        by=['_has_type', date_column],
        ascending=[True, True],
        na_position='last'
    )

    # Remove temporary column
    df = df.drop(columns=['_has_type'])

    return df
