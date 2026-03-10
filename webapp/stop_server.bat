@echo off
chcp 65001 >nul
echo ════════════════════════════════════════
echo   Audit Manager — остановка
echo ════════════════════════════════════════

REM Поиск по заголовку окна (если запущен через start_server.bat)
taskkill /FI "WINDOWTITLE eq AuditManager" /F >nul 2>&1

REM Поиск по порту 8080 (универсальный способ)
for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":8080.*LISTENING"') do (
    taskkill /PID %%a /F >nul 2>&1
)

echo.
echo   Сервер остановлен.
echo ════════════════════════════════════════
timeout /t 2 >nul
