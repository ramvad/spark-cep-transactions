@echo off
echo "Building Spark CEP Transaction Detector..."
call mvn clean package -DskipTests

if %ERRORLEVEL% EQU 0 (
    echo "Build successful! Running batch processor..."
    java --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-exports=java.base/sun.nio.ch=ALL-UNNAMED -cp target/spark-cep-transactions-1.0.jar com.example.SparkCEPTransactionBatch
) else (
    echo "Build failed!"
    pause
)
