@echo off
cd /d C:\Users\MagicTouch\Desktop\repos\case_locations
uv run python sync/mssql_to_postgres.py >> logs\sync_log.txt 2>&1
