@echo off
echo "Ultimate simple test - using only the essential JVM flag..."

set MAVEN_OPTS=--add-opens=java.base/sun.nio.ch=ALL-UNNAMED

echo "Running batch test with single JVM flag..."
mvn exec:java -Dexec.mainClass=com.example.SparkCEPTransactionBatch

pause
