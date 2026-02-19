@echo off
setlocal
cd /d "%~dp0.."
call "%~dp0..\etl\ga4_sync_daily.cmd" --lookback 30
exit /b %errorlevel%