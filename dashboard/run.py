"""Entry point: uvicorn launcher for the dashboard."""
import uvicorn
from dashboard.config import DASHBOARD_PORT, DASHBOARD_HOST

if __name__ == "__main__":
    uvicorn.run(
        "dashboard.app:app",
        host=DASHBOARD_HOST,
        port=DASHBOARD_PORT,
        reload=False,
    )
