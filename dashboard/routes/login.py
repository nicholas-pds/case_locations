from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from dashboard.auth import check_password, create_session_cookie

router = APIRouter()


@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, error: str = None):
    templates = request.app.state.templates
    return templates.TemplateResponse("login.html", {
        "request": request,
        "error": error,
    })


@router.post("/login")
async def login_submit(request: Request, password: str = Form(...)):
    if check_password(password):
        cookie_value, max_age = create_session_cookie()
        response = RedirectResponse(url="/", status_code=302)
        response.set_cookie(
            key="dashboard_session",
            value=cookie_value,
            max_age=int(max_age),
            httponly=True,
            samesite="lax",
        )
        return response
    else:
        templates = request.app.state.templates
        return templates.TemplateResponse("login.html", {
            "request": request,
            "error": "Incorrect password",
        })


@router.get("/logout")
async def logout():
    response = RedirectResponse(url="/login", status_code=302)
    response.delete_cookie("dashboard_session")
    return response
