@echo off
echo "=== RUNNING SPARK CEP TEST ==="
echo Current directory: %CD%
echo Java version:
java -version
echo.
echo Maven version:
mvn -version
echo.
echo Setting JVM options...
set MAVEN_OPTS=--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
echo MAVEN_OPTS set to: %MAVEN_OPTS%
echo.
echo Available classes:
dir target\classes\com\example\*.class
echo.
echo Running TestMain...
mvn exec:java -Dexec.mainClass=com.example.TestMain -e
echo.
echo Done.
pause
