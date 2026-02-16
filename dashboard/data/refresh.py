"""Background task that refreshes the data cache every 60 seconds."""
import asyncio
import logging
from datetime import datetime
from .cache import cache
from .queries import (
    fetch_case_locations,
    fetch_workload_status,
    fetch_workload_pivot,
    fetch_airway_workflow,
    fetch_airway_hold_status,
    fetch_submitted_cases,
    fetch_daily_sales,
)
from .transforms import add_filter_columns
from dashboard.config import REFRESH_INTERVAL_SECONDS, BUSINESS_HOURS_START, BUSINESS_HOURS_END

logger = logging.getLogger("dashboard.refresh")

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
    ]

    for name, result in zip(names, results):
        if isinstance(result, Exception):
            logger.error(f"Query '{name}' failed: {result}")
            continue

        # Add filter columns to case_locations
        if name == "case_locations" and not result.empty:
            result = add_filter_columns(result)

        await cache.set(name, result)
        logger.info(f"Cache updated: {name} ({len(result)} rows)")

    await cache.set_last_refresh(datetime.now())
    await _notify_subscribers()


async def refresh_loop():
    """Main background refresh loop. Runs every REFRESH_INTERVAL_SECONDS."""
    logger.info("Background refresh task started")

    while True:
        if _is_business_hours():
            await cache.set_paused(False)
            try:
                logger.info("Refreshing data...")
                await refresh_all_queries()
                logger.info("Refresh complete")
            except Exception as e:
                logger.error(f"Refresh failed: {e}")
                await cache.set_error(str(e))
        else:
            await cache.set_paused(True)
            logger.debug("Outside business hours, skipping refresh")

        await asyncio.sleep(REFRESH_INTERVAL_SECONDS)
