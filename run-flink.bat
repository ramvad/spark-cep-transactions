@echo off
setlocal EnableDelayedExpansion

REM === Configure paths ===
set FLINK_HOME=C:\Users\svgks\flink-1.17.1
set FLINK_LIB=%FLINK_HOME%\lib
set JAR_PATH=C:\Users\svgks\Downloads\flink-cep-transactions\target\flink-cep-transactions-1.0.jar
set MAIN_CLASS=com.example.FlinkCEPTransactionDetector
set JAVA17_HOME="C:\Program Files\Java\jdk-17"

echo Running Flink job from: %JAR_PATH%
echo Using JVM options: --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED
echo === Starting Flink Job in Local Mode ===

REM === Confirm Java version ===
%JAVA17_HOME%\bin\java.exe -version

REM === Construct classpath ===
set CP=
for %%f in (%FLINK_LIB%\*.jar) do (
    set CP=!CP!;%%f
)

REM Remove leading semicolon
set CP=%CP:~1%

REM === Run Flink CLI to submit the job ===
%JAVA17_HOME%\bin\java.exe ^
--add-opens java.base/java.lang=ALL-UNNAMED ^
--add-opens java.base/java.util=ALL-UNNAMED ^
-Xmx1024m ^
-cp "%CP%" ^
org.apache.flink.client.cli.CliFrontend run -c %MAIN_CLASS% "%JAR_PATH%"

pause

