@echo off
set "BUNDLE=%~dp0."
"%~dp0runtime\urban-import.exe" worker-export --bundle "%BUNDLE%"
exit /b %ERRORLEVEL%
