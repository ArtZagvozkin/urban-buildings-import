@echo off
set "BUNDLE=%~dp0."
"%~dp0runtime\urban-import.exe" worker-stop --bundle "%BUNDLE%"
exit /b %ERRORLEVEL%
