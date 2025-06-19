@echo off
echo "Testing minimal Spark setup..."

set MAVEN_OPTS=--add-opens=java.base/sun.nio.ch=ALL-UNNAMED

echo "Compiling first..."
mvn compile

echo "Running minimal test..."
mvn exec:java -Dexec.mainClass=com.example.TestMain

pause
