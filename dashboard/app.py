"""FastAPI application factory."""
import os
import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, date
import pandas as pd
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from dashboard.config import TEMPLATE_DIR, STATIC_DIR
from dashboard.auth import AuthMiddleware
from dashboard.data.refresh import refresh_loop

# Windows uses %#I instead of %-I for non-zero-padded hour
_TIME_FMT = '%#I:%M %p' if os.name == 'nt' else '%-I:%M %p'
_DATE_SHORT_FMT = '%#m/%#d' if os.name == 'nt' else '%-m/%-d'

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("dashboard")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start background refresh task on startup."""
    logger.info("Starting dashboard server...")
    task = asyncio.create_task(refresh_loop())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    logger.info("Dashboard server stopped.")


def create_app() -> FastAPI:
    app = FastAPI(title="Partners Case Locations Dashboard", lifespan=lifespan)

    # Auth middleware
    app.add_middleware(AuthMiddleware)

    # Static files
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    # Templates
    templates = Jinja2Templates(directory=str(TEMPLATE_DIR))

    # Custom Jinja2 filters for date/time formatting
    def fmt_time(value):
        """Format timestamp: time only if today, otherwise time + date."""
        if pd.isna(value) or value is None or value == '':
            return ''
        try:
            if isinstance(value, str):
                value = pd.to_datetime(value)
            if hasattr(value, 'strftime'):
                if hasattr(value, 'date') and value.date() == date.today():
                    return value.strftime(_TIME_FMT)
                return value.strftime(_TIME_FMT) + ' ' + value.strftime(_DATE_SHORT_FMT)
        except Exception:
            pass
        return str(value)

    def fmt_date(value):
        """Format date to '2/11'."""
        if pd.isna(value) or value is None or value == '':
            return ''
        try:
            if isinstance(value, str):
                value = pd.to_datetime(value)
            if hasattr(value, 'date') and not isinstance(value, date):
                value = value.date()
            if hasattr(value, 'strftime'):
                return value.strftime(_DATE_SHORT_FMT)
        except Exception:
            pass
        return str(value)

    def fmt_datetime(value):
        """Format timestamp to '2:56 PM 02/11'."""
        if pd.isna(value) or value is None or value == '':
            return ''
        try:
            if isinstance(value, str):
                value = pd.to_datetime(value)
            if hasattr(value, 'strftime'):
                return value.strftime(f'{_TIME_FMT} %m/%d')
        except Exception:
            pass
        return str(value)

    templates.env.filters['fmt_time'] = fmt_time
    templates.env.filters['fmt_date'] = fmt_date
    templates.env.filters['fmt_datetime'] = fmt_datetime

    app.state.templates = templates

    # Register routes
    from dashboard.routes.main_page import router as main_router
    from dashboard.routes.workload import router as workload_router
    from dashboard.routes.airway_workflow import router as airway_workflow_router
    from dashboard.routes.airway_hold import router as airway_hold_router
    from dashboard.routes.local_delivery import router as local_delivery_router
    from dashboard.routes.overdue_noscan import router as overdue_noscan_router
    from dashboard.routes.daily_summary import router as daily_summary_router
    from dashboard.routes.customers import router as customers_router
    from dashboard.routes.partials import router as partials_router
    from dashboard.routes.sse import router as sse_router
    from dashboard.routes.login import router as login_router

    app.include_router(login_router)
    app.include_router(main_router)
    app.include_router(workload_router)
    app.include_router(airway_workflow_router)
    app.include_router(airway_hold_router)
    app.include_router(local_delivery_router)
    app.include_router(overdue_noscan_router)
    app.include_router(daily_summary_router)
    app.include_router(customers_router)
    app.include_router(partials_router)
    app.include_router(sse_router)

    return app


app = create_app()
