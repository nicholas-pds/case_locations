"""Simple shared-password authentication with cookie sessions."""
import hashlib
import hmac
import time
from starlette.requests import Request
from starlette.responses import RedirectResponse
from starlette.middleware.base import BaseHTTPMiddleware
from dashboard.config import DASHBOARD_PASSWORD, SECRET_KEY, SESSION_MAX_AGE


def _make_token(timestamp: str) -> str:
    """Create an HMAC token from timestamp."""
    msg = f"dashboard-session:{timestamp}"
    return hmac.new(SECRET_KEY.encode(), msg.encode(), hashlib.sha256).hexdigest()


def create_session_cookie() -> tuple[str, str]:
    """Returns (cookie_value, max_age) for a valid session."""
    ts = str(int(time.time()))
    token = _make_token(ts)
    return f"{ts}:{token}", str(SESSION_MAX_AGE)


def verify_session_cookie(cookie_value: str) -> bool:
    """Check if a session cookie is valid and not expired."""
    if not cookie_value:
        return False
    try:
        parts = cookie_value.split(":", 1)
        if len(parts) != 2:
            return False
        ts_str, token = parts
        ts = int(ts_str)
        # Check expiry
        if time.time() - ts > SESSION_MAX_AGE:
            return False
        # Check signature
        expected = _make_token(ts_str)
        return hmac.compare_digest(token, expected)
    except (ValueError, TypeError):
        return False


def check_password(password: str) -> bool:
    """Verify the shared password."""
    return password == DASHBOARD_PASSWORD


class AuthMiddleware(BaseHTTPMiddleware):
    """Middleware that checks for valid session cookie on all routes except /login and /static."""

    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        # Allow login page, static files, and favicon
        if path.startswith("/login") or path.startswith("/static") or path == "/favicon.ico":
            return await call_next(request)

        # Check session cookie
        session_cookie = request.cookies.get("dashboard_session")
        if not verify_session_cookie(session_cookie):
            return RedirectResponse(url="/login", status_code=302)

        return await call_next(request)
