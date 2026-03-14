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

:: Запускаем через python с перенаправлением вывода в лог
:: (pythonw глотает ошибки — используем python + скрытое окно)
start "AuditManager" /MIN python main.py > server.log 2>&1

echo.
echo   Сервер запускается...

:: Ждём пока сервер стартует (проверяем порт каждую секунду)
set TRIES=0
:wait_loop
timeout /t 1 >nul
set /a TRIES+=1
netstat -ano | findstr ":8080.*LISTENING" >nul 2>&1
if not errorlevel 1 goto server_up
if %TRIES% GEQ 10 goto server_fail
echo   Ожидание... (%TRIES%/10)
goto wait_loop

:server_up
echo.
echo   Сервер запущен!
start "" http://localhost:8080
echo   Браузер открыт: http://localhost:8080
echo   Чтобы остановить — запустите stop_server.bat
echo   Лог: %~dp0server.log
echo ════════════════════════════════════════
timeout /t 3 >nul
exit /b

:server_fail
echo.
echo   ОШИБКА: Сервер не запустился за 10 секунд!
echo   Проверьте лог: %~dp0server.log
echo.
if exist server.log (
    echo --- Последние строки лога ---
    type server.log
    echo ---
)
echo ════════════════════════════════════════
pause
