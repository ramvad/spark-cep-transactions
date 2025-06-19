@echo off
echo "Testing with minimal Spark configuration..."

echo "Setting minimal JVM args..."
set MAVEN_OPTS=-Djava.security.manager=default --add-opens java.base/sun.nio.ch=ALL-UNNAMED

echo "Running simple batch test..."
mvn exec:java -Dexec.mainClass=com.example.SparkCEPTransactionBatch -Dexec.args="" -Dspark.serializer=org.apache.spark.serializer.JavaSerializer

pause
