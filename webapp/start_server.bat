@echo off
chcp 65001 >nul
echo ════════════════════════════════════════
echo   Audit Manager — запуск в фоне
echo ════════════════════════════════════════

cd /d "%~dp0"

:: Проверяем, не запущен ли уже (по порту 8080)
netstat -ano | findstr ":8080.*LISTENING" >nul 2>&1
if not errorlevel 1 (
    echo.
    echo   Сервер уже запущен!
    start "" http://localhost:8080
    echo   Браузер открыт.
    echo.
    timeout /t 2 >nul
    exit /b
)

:: Запускаем скрыто через pythonw (без окна консоли)
start "" /B pythonw main.py

echo.
echo   Сервер запускается...
echo.

:: Ждём пока сервер стартует, затем открываем браузер
timeout /t 2 >nul
start "" http://localhost:8080

echo   Браузер открыт: http://localhost:8080
echo   Чтобы остановить — запустите stop_server.bat
echo ════════════════════════════════════════
timeout /t 3 >nul
