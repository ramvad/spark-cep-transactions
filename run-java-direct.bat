@echo off
echo "Alternative approach: Using java command directly with proper classpath..."

echo "Building project first..."
call mvn clean compile dependency:build-classpath -Dmdep.outputFile=classpath.txt

if exist classpath.txt (
    echo "Running with direct java command..."
    set /p CLASSPATH=<classpath.txt
    java --add-opens=java.base/java.lang=ALL-UNNAMED ^
         --add-opens=java.base/java.lang.invoke=ALL-UNNAMED ^
         --add-opens=java.base/java.lang.reflect=ALL-UNNAMED ^
         --add-opens=java.base/java.io=ALL-UNNAMED ^
         --add-opens=java.base/java.net=ALL-UNNAMED ^
         --add-opens=java.base/java.nio=ALL-UNNAMED ^
         --add-opens=java.base/java.util=ALL-UNNAMED ^
         --add-opens=java.base/java.util.concurrent=ALL-UNNAMED ^
         --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED ^
         --add-opens=java.base/sun.nio.ch=ALL-UNNAMED ^
         --add-opens=java.base/sun.nio.cs=ALL-UNNAMED ^
         --add-opens=java.base/sun.security.action=ALL-UNNAMED ^
         --add-opens=java.base/sun.util.calendar=ALL-UNNAMED ^
         --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED ^
         -cp "target/classes;%CLASSPATH%" com.example.SparkCEPTransactionBatch
) else (
    echo "Failed to generate classpath"
)

pause
