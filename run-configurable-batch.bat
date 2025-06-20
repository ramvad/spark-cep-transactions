@echo off
echo ===============================================
echo Configurable Spark CEP Transaction Batch
echo ===============================================
echo.

REM Check if Java is available
java -version >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo ERROR: Java is not installed or not in PATH
    echo Please install Java 17 and try again
    pause
    exit /b 1
)

REM Check if Maven is available
mvn -version >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo ERROR: Maven is not installed or not in PATH
    echo Please install Maven and try again
    pause
    exit /b 1
)

echo [INFO] Building project with Maven...
mvn clean compile -q
if %ERRORLEVEL% neq 0 (
    echo ERROR: Maven build failed
    pause
    exit /b 1
)

echo [INFO] Configuration file: fraud-patterns-config.json
if not exist "fraud-patterns-config.json" (
    echo WARNING: Configuration file not found, using default patterns
)

echo [INFO] Starting Configurable Spark CEP Transaction Batch...
echo.

REM Run the configurable batch processor
mvn exec:java -Dexec.mainClass="com.example.ConfigurableSparkCEPBatch" -Dexec.args="fraud-patterns-config.json" ^
    -Dexec.args="--add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED --add-exports java.base/sun.nio.ch=ALL-UNNAMED" ^
    -q

if %ERRORLEVEL% equ 0 (
    echo.
    echo ===============================================
    echo Configurable Spark CEP Batch completed successfully!
    echo ===============================================
    echo.
    echo Generated files:
    if exist "fraud_alerts" (
        echo - Fraud alerts: fraud_alerts/
    )
    if exist "configurable-spark-cep-batch-*.log" (
        echo - Log files: configurable-spark-cep-batch-*.log
    )
    echo.
) else (
    echo.
    echo ===============================================
    echo ERROR: Configurable Spark CEP Batch failed!
    echo ===============================================
    echo Please check the error messages above.
    echo.
)

echo Press any key to exit...
pause >nul
