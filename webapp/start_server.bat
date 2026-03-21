@echo off
setlocal
chcp 65001 >nul

set "SCRIPT_DIR=%~dp0"
set "ROOT_DIR=%~dp0.."
set "PYTHON_EXE=C:\Users\uzun.a.i\AppData\Local\Programs\Python\Python313\python.exe"
if not exist "%PYTHON_EXE%" (
    set "PYTHON_EXE=python"
)

echo ========================================
echo   Audit Manager background start
echo ========================================

netstat -ano | findstr ":8080.*LISTENING" >nul 2>&1
if not errorlevel 1 (
    echo Server is already running on http://localhost:8080
    exit /b 0
)

if exist "%SCRIPT_DIR%server.log" del /q "%SCRIPT_DIR%server.log" >nul 2>&1
if exist "%SCRIPT_DIR%server.err.log" del /q "%SCRIPT_DIR%server.err.log" >nul 2>&1

powershell -NoProfile -ExecutionPolicy Bypass -Command "Start-Process -FilePath '%PYTHON_EXE%' -ArgumentList '-m','webapp.main' -WorkingDirectory '%ROOT_DIR%' -RedirectStandardOutput '%SCRIPT_DIR%server.log' -RedirectStandardError '%SCRIPT_DIR%server.err.log' -WindowStyle Minimized"

echo Waiting for server...
set "TRIES=0"
:wait_loop
timeout /t 1 >nul
set /a TRIES+=1
netstat -ano | findstr ":8080.*LISTENING" >nul 2>&1
if not errorlevel 1 goto server_up
if %TRIES% GEQ 15 goto server_fail
echo   wait %TRIES%/15
goto wait_loop

:server_up
echo Server is up: http://localhost:8080
echo Stdout log: %SCRIPT_DIR%server.log
echo Stderr log: %SCRIPT_DIR%server.err.log
exit /b 0

:server_fail
echo ERROR: server did not start in time
if exist "%SCRIPT_DIR%server.err.log" (
    type "%SCRIPT_DIR%server.err.log"
)
exit /b 1
