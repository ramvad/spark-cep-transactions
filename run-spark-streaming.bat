@echo off
echo "Building Spark CEP Transaction Detector..."
call mvn clean package -DskipTests

if %ERRORLEVEL% EQU 0 (
    echo "Build successful! Running streaming processor..."
    call mvn exec:java -Dexec.mainClass=com.example.SparkCEPTransactionDetector
) else (
    echo "Build failed!"
    pause
)
