@echo off
echo ===============================================
echo  Spark CEP Transaction Detector
echo  Complete Batch Processing Solution
echo ===============================================
echo.

REM Build project if needed
if not exist "target\spark-cep-transactions-1.0.jar" (
    echo Building project...
    call mvn clean package -q
    if %ERRORLEVEL% NEQ 0 (
        echo Build failed! Check Maven setup.
        pause
        exit /b 1
    )
    echo Build successful!
) else (
    echo Project already built.
)
echo.

echo What would you like to do?
echo.
echo [1] Run fraud detection analysis
echo [2] Scheduled analysis (every 15 minutes)
echo [3] View sample data
echo [4] View recent logs
echo [5] Exit
echo.
set /p choice="Enter choice (1-5): "

if "%choice%"=="1" goto single_run
if "%choice%"=="2" goto scheduled_run
if "%choice%"=="3" goto show_data
if "%choice%"=="4" goto show_logs
if "%choice%"=="5" goto exit
goto invalid_choice

:single_run
echo.
echo === RUNNING FRAUD DETECTION ANALYSIS ===
echo Analyzing transactions for suspicious patterns...
echo.
java --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-exports=java.base/sun.nio.ch=ALL-UNNAMED -cp target/spark-cep-transactions-1.0.jar com.example.SparkCEPTransactionBatch
echo.
echo Analysis complete! Check the log file for detailed results.
goto end

:scheduled_run
echo.
echo === SCHEDULED FRAUD DETECTION ===
echo Running analysis every 15 minutes. Press Ctrl+C to stop.
echo.
:loop
echo [%TIME%] Running analysis...
java --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-exports=java.base/sun.nio.ch=ALL-UNNAMED -cp target/spark-cep-transactions-1.0.jar com.example.SparkCEPTransactionBatch
echo [%TIME%] Complete. Waiting 15 minutes...
timeout /t 900 /nobreak >nul
goto loop

:show_data
echo.
echo === SAMPLE TRANSACTION DATA ===
if exist transactions.csv (
    type transactions.csv
) else (
    echo transactions.csv not found!
)
goto end

:show_logs
echo.
echo === RECENT LOG FILES ===
for %%f in (spark-cep-batch-*.log) do echo %%f
goto end

:invalid_choice
echo Invalid choice. Please try again.
pause
exit /b 1

:exit
echo Goodbye!
exit /b 0

:end
echo.
echo Done! Use Windows Task Scheduler for production scheduling.
pause
