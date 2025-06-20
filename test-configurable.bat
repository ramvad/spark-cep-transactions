@echo off
echo Testing Configurable Spark CEP Batch...

REM Set JVM arguments for Java 17 compatibility
set JAVA_OPTS=--add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED

REM Run the configurable batch
java %JAVA_OPTS% -cp "target/classes;target/dependency/*" com.example.ConfigurableSparkCEPBatch fraud-patterns-config.json

echo.
echo Test completed.
pause
