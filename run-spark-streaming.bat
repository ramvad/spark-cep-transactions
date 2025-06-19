@echo off
echo "Building Spark CEP Transaction Detector..."
call mvn clean package -DskipTests -q

if %ERRORLEVEL% EQU 0 (
    echo "Build successful! Running streaming processor..."
    echo.
    echo "=== SPARK CEP STREAMING MODE ==="
    echo "This will start streaming CEP analysis with:"
    echo "- Rate source: 1 transaction per second"
    echo "- Window analysis: 2-minute sliding windows every 30 seconds"
    echo "- Alert triggers: 2+ transactions totaling >$500"
    echo "- Processing intervals: 5-10 seconds"
    echo.
    echo "You will see:"
    echo "1. Processed transactions every 5 seconds"
    echo "2. Fraud alerts every 10 seconds (when detected)"
    echo "3. Log file created with detailed analysis"
    echo.
    echo "Press Ctrl+C to stop the streaming..."
    echo.
    
    java --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -cp target/spark-cep-transactions-1.0.jar com.example.SparkCEPTransactionDetector
) else (
    echo "Build failed!"
    pause
)
