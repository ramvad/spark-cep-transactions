@echo off
echo "Testing Spark CEP Streaming Transaction Detector..."
echo "Building project..."
call mvn clean package -q

if %ERRORLEVEL% EQU 0 (
    echo "Build successful! Testing streaming processor..."
    echo "NOTE: Running streaming CEP with rate source (1 transaction/second)"
    echo "This will cycle through the transaction data and detect patterns."
    echo "Press Ctrl+C to stop the streaming when you see some results."
    echo.
    
    echo "Starting streaming in 3 seconds..."
    timeout /t 3 /nobreak > nul
    
    java --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -cp target/spark-cep-transactions-1.0.jar com.example.SparkCEPTransactionDetector
    
    echo.
    echo "Streaming stopped!"
    echo "Check the generated spark-cep-streaming-*.log file for detailed results."
) else (
    echo "Build failed!"
)

pause
