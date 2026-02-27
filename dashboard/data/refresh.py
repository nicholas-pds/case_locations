"""Background task that refreshes the data cache every 60 seconds."""
import asyncio
import logging
from datetime import datetime, date
from .cache import cache
from .queries import (
    fetch_case_locations,
    fetch_workload_status,
    fetch_workload_pivot,
    fetch_airway_workflow,
    fetch_airway_hold_status,
    fetch_submitted_cases,
    fetch_daily_sales,
    fetch_customers,
    fetch_monthly_sales,
)
from .transforms import add_filter_columns
from dashboard.config import REFRESH_INTERVAL_SECONDS, BUSINESS_HOURS_START, BUSINESS_HOURS_END
from src.holidays import get_all_company_holidays

logger = logging.getLogger("dashboard.refresh")

# Track which midday jobs have already fired today (reset at midnight)
_midday_jobs_fired: set = set()  # values: ('noon', date) or ('3pm', date)

# SSE subscribers - routes/sse.py will register callbacks here
_subscribers: list = []


def subscribe(callback):
    _subscribers.append(callback)


def unsubscribe(callback):
    if callback in _subscribers:
        _subscribers.remove(callback)


async def _notify_subscribers():
    for cb in _subscribers[:]:
        try:
            await cb()
        except Exception:
            pass


def _is_business_hours() -> bool:
    now = datetime.now()
    return BUSINESS_HOURS_START <= now.hour < BUSINESS_HOURS_END


async def refresh_all_queries():
    """Execute all SQL queries and update the cache."""
    loop = asyncio.get_event_loop()

    # Run all queries concurrently in thread pool (pyodbc is synchronous)
    results = await asyncio.gather(
        loop.run_in_executor(None, fetch_case_locations),
        loop.run_in_executor(None, fetch_workload_status),
        loop.run_in_executor(None, fetch_workload_pivot),
        loop.run_in_executor(None, fetch_airway_workflow),
        loop.run_in_executor(None, fetch_airway_hold_status),
        loop.run_in_executor(None, fetch_submitted_cases),
        loop.run_in_executor(None, fetch_daily_sales),
        loop.run_in_executor(None, fetch_customers),
        loop.run_in_executor(None, fetch_monthly_sales),
        return_exceptions=True,
    )

    names = [
        "case_locations",
        "workload_status",
        "workload_pivot",
        "airway_workflow",
        "airway_hold_status",
        "submitted_cases",
        "daily_sales",
        "customers",
        "monthly_sales",
    ]

    for name, result in zip(names, results):
        if isinstance(result, Exception):
            logger.error(f"Query '{name}' failed: {result}")
            continue

        # Add filter columns to case_locations
        if name == "case_locations" and not result.empty:
            try:
                result = add_filter_columns(result)
            except Exception as e:
                logger.error(f"Transform '{name}' failed: {e}")
                continue

        await cache.set(name, result)
        logger.info(f"Cache updated: {name} ({len(result)} rows)")

    await cache.set_last_refresh(datetime.now())
    await _notify_subscribers()


def _is_business_day(d: date = None) -> bool:
    """Return True if d (default today) is a weekday and not a company holiday."""
    if d is None:
        d = date.today()
    if d.weekday() >= 5:
        return False
    return d not in get_all_company_holidays()


async def _run_midday_job(window: str) -> None:
    """Run a midday snapshot job ('noon' or '3pm') in the thread pool."""
    loop = asyncio.get_event_loop()
    try:
        from dashboard.data.efficiency_processing import process_midday_snapshot
        from dashboard.data.efficiency_store import save_midday
        logger.info(f"Running midday job: {window}")
        df = await loop.run_in_executor(None, process_midday_snapshot, window)
        await loop.run_in_executor(None, save_midday, window, df)
        logger.info(f"Midday job '{window}' complete: {len(df)} rows")
    except Exception as e:
        logger.error(f"Midday job '{window}' failed: {e}")


async def refresh_loop():
    """Main background refresh loop. Runs every REFRESH_INTERVAL_SECONDS."""
    logger.info("Background refresh task started")
    _consecutive_failures = 0

    while True:
        now = datetime.now()
        today = now.date()

        if _is_business_hours():
            await cache.set_paused(False)
            try:
                logger.info("Refreshing data...")
                await refresh_all_queries()
                logger.info("Refresh complete")
                if _consecutive_failures > 0:
                    logger.info(f"Database connection restored after {_consecutive_failures} failure(s)")
                _consecutive_failures = 0
            except asyncio.CancelledError:
                logger.info("Refresh loop cancelled — shutting down")
                raise
            except Exception as e:
                _consecutive_failures += 1
                # Log at ERROR on first failure, then WARN every 5 to avoid log spam
                if _consecutive_failures == 1 or _consecutive_failures % 5 == 0:
                    logger.error(f"Refresh failed (attempt {_consecutive_failures}): {e}")
                await cache.set_error(str(e))
        else:
            await cache.set_paused(True)
            logger.debug("Outside business hours, skipping refresh")

        # Midday scheduled jobs (noon and 3PM) — only on business days
        if _is_business_day(today):
            # Noon job: fires between 12:00:00 and 12:00:59
            noon_key = ("noon", today)
            if now.hour == 12 and now.minute == 0 and noon_key not in _midday_jobs_fired:
                _midday_jobs_fired.add(noon_key)
                asyncio.create_task(_run_midday_job("noon"))

            # 3PM job: fires between 15:00:00 and 15:00:59
            pm3_key = ("3pm", today)
            if now.hour == 15 and now.minute == 0 and pm3_key not in _midday_jobs_fired:
                _midday_jobs_fired.add(pm3_key)
                asyncio.create_task(_run_midday_job("3pm"))

            # Clean up old keys (keep only today's)
            old_keys = {k for k in _midday_jobs_fired if k[1] != today}
            _midday_jobs_fired.difference_update(old_keys)

        await asyncio.sleep(REFRESH_INTERVAL_SECONDS)
