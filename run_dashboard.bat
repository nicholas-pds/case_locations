@echo off
REM Start the Partners Dental Case Locations Dashboard
REM The dashboard will be available at http://localhost:8050
REM Press Ctrl+C to stop the server.
cd /d "%~dp0"
powershell.exe -Command "uv run python -m dashboard.run"
pause
