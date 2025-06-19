@echo off
echo "Building Spark CEP Transaction Detector..."
call mvn clean package -DskipTests

if %ERRORLEVEL% EQU 0 (
    echo "Build successful! Running batch processor..."
    call mvn exec:java -Dexec.mainClass=com.example.SparkCEPTransactionBatch
) else (
    echo "Build failed!"
    pause
)
