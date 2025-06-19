package com.example;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static org.apache.spark.sql.functions.*;

public class TestMain {
    
    private static PrintWriter logWriter;
    
    // Helper method to log to both console and file
    private static void logAndPrint(String message) {
        System.out.println(message);
        if (logWriter != null) {
            logWriter.println(LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + " - " + message);
            logWriter.flush();
        }
    }
    
    public static void main(String[] args) {
        
        // Initialize log file
        try {
            String logFileName = "spark-test-" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss")) + ".log";
            logWriter = new PrintWriter(new FileWriter(logFileName, true));
            logAndPrint("=== MINIMAL SPARK TEST - Log file: " + logFileName + " ===");
        } catch (IOException e) {
            System.err.println("Failed to initialize log file: " + e.getMessage());
        }
        
        try {
            // Create the simplest possible Spark session
            SparkSession spark = SparkSession.builder()
                    .appName("Minimal Test")
                    .master("local[1]")
                    .config("spark.ui.enabled", "false")
                    .config("spark.sql.adaptive.enabled", "false")
                    .getOrCreate();
            
            spark.sparkContext().setLogLevel("ERROR");
            
            logAndPrint("[OK] Spark session created successfully!");
            
            // Test basic functionality
            Dataset<Row> df = spark.read()
                    .format("csv")
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .load("transactions.csv");
            
            logAndPrint("[OK] CSV file loaded successfully!");
            logAndPrint("Row count: " + df.count());
            
            df.show(5);
            
            // Test basic CEP logic
            Dataset<Row> processed = df
                    .withColumn("eventTime", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
                    .withColumn("amount", col("transactionAmount"))
                    .select("accountId", "amount", "eventTime")
                    .orderBy("accountId", "eventTime");
            
            logAndPrint("[OK] Data processing successful!");
            processed.show();
            
            // Test window function
            Dataset<Row> windowed = processed
                    .withColumn("rowNum", 
                            row_number().over(
                                    Window.partitionBy("accountId")
                                          .orderBy("eventTime")
                            )
                    );
            
            logAndPrint("[OK] Window functions working!");
            windowed.show();
              spark.stop();
            logAndPrint("[OK] ALL TESTS PASSED!");
            
        } catch (Exception e) {
            System.err.println("[ERROR] " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (logWriter != null) {
                logWriter.close();
            }
        }
    }
}

