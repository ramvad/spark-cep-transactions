@echo off
echo Starting Flink CEP Transaction Detector...

set FLINK_HOME=C:\Users\svgks\flink-1.17.1
set JAR_PATH=C:\Users\svgks\Downloads\flink-cep-transactions\target\flink-cep-transactions-1.0.jar

java -Dlog4j.configurationFile=src/main/resources/log4j2.properties ^
     --add-opens java.base/java.lang=ALL-UNNAMED ^
     --add-opens java.base/java.util=ALL-UNNAMED ^
     -cp "%JAR_PATH%;%FLINK_HOME%\lib\*" ^
     com.example.FlinkCEPTransactionDetector > cep_bat_output.log 2>&1

if errorlevel 1 (
    echo Error running Flink application
    pause
    exit /b 1
)

echo Flink application completed successfully
pause