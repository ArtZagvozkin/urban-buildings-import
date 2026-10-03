@echo off
setlocal
set "BUNDLE=%~dp0."
if defined REQUESTS_CA_BUNDLE if not exist "%REQUESTS_CA_BUNDLE%" set "REQUESTS_CA_BUNDLE="
if defined CURL_CA_BUNDLE if not exist "%CURL_CA_BUNDLE%" set "CURL_CA_BUNDLE="
if defined SSL_CERT_FILE if not exist "%SSL_CERT_FILE%" set "SSL_CERT_FILE="
"%~dp0runtime\urban-import.exe" worker-run --bundle "%BUNDLE%" %*
exit /b %ERRORLEVEL%
