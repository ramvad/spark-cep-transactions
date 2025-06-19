package com.example;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static org.apache.spark.sql.functions.*;

public class SparkCEPTransactionBatch {

    private static final Logger LOG = LoggerFactory.getLogger(SparkCEPTransactionBatch.class);
    private static PrintWriter logWriter;
      // Helper method to log to both console and file
    private static void logAndPrint(String message) {
        System.out.println(message);
        LOG.info(message);
        if (logWriter != null) {
            logWriter.println(LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + " - " + message);
            logWriter.flush();
        }
    }
    
    // Helper method to show DataFrame and log its content
    private static void showAndLog(Dataset<Row> df, boolean truncate) {
        // Show to console
        df.show(20, truncate);
        
        // Capture and log the content
        if (logWriter != null) {
            try {
                String timestamp = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                logWriter.println(timestamp + " - DataFrame Content:");
                
                // Get the schema info
                logWriter.println(timestamp + " - Schema: " + df.schema().treeString());
                
                // Collect and log the data (limit to reasonable size)
                Row[] rows = (Row[]) df.collect();
                if (rows.length > 0) {
                    // Log column headers
                    String[] columnNames = df.columns();
                    logWriter.println(timestamp + " - Columns: " + String.join(", ", columnNames));
                    
                    // Log each row
                    for (int i = 0; i < Math.min(rows.length, 50); i++) { // Limit to 50 rows
                        StringBuilder rowStr = new StringBuilder();
                        for (int j = 0; j < columnNames.length; j++) {
                            if (j > 0) rowStr.append(" | ");
                            Object value = rows[i].get(j);
                            rowStr.append(value != null ? value.toString() : "null");
                        }
                        logWriter.println(timestamp + " - Row " + (i+1) + ": " + rowStr.toString());
                    }
                    if (rows.length > 50) {
                        logWriter.println(timestamp + " - ... (" + (rows.length - 50) + " more rows)");
                    }
                } else {
                    logWriter.println(timestamp + " - No data rows");
                }
                logWriter.println(timestamp + " - Total rows: " + df.count());
                logWriter.flush();
            } catch (Exception e) {
                logWriter.println(LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + " - Error logging DataFrame: " + e.getMessage());
                logWriter.flush();
            }
        }
    }
    
    // Overloaded method with default truncate=true
    private static void showAndLog(Dataset<Row> df) {
        showAndLog(df, true);
    }
      public static void main(String[] args) {
        
        // Initialize log file
        try {
            String logFileName = "spark-cep-batch-" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss")) + ".log";
            logWriter = new PrintWriter(new FileWriter(logFileName, true));
            logAndPrint(">>> SparkCEPTransactionBatch start - Log file: " + logFileName);
        } catch (IOException e) {
            System.err.println("Failed to initialize log file: " + e.getMessage());
            LOG.error("Failed to initialize log file", e);
        }
        
        logAndPrint(">>> SparkCEPTransactionBatch starting...");
        LOG.info(">>> SparkCEPTransactionBatch starting...");
        
        // Create Spark Session
        SparkSession spark = SparkSession.builder()
                .appName("Spark CEP Transaction Batch")
                .master("local[*]")
                .getOrCreate();
        
        // Set log level to reduce noise
        spark.sparkContext().setLogLevel("WARN");
          // Read CSV file
        Dataset<Row> rawTransactions = spark
                .read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("transactions.csv");        logAndPrint("=== RAW TRANSACTIONS ===");
        showAndLog(rawTransactions);
        
        // Convert timestamp and prepare data
        Dataset<Row> transactions = rawTransactions
                .withColumn("eventTime", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
                .withColumn("amount", col("transactionAmount"))
                .select("accountId", "amount", "eventTime")
                .filter(col("amount").gt(0))
                .orderBy("accountId", "eventTime");
          logAndPrint("=== PROCESSED TRANSACTIONS ===");
        showAndLog(transactions);
          // Method 1: Window-based analysis
        logAndPrint("=== WINDOW-BASED ANALYSIS ===");
        Dataset<Row> windowedResults = transactions
                .groupBy(
                        col("accountId"),
                        window(col("eventTime"), "1 hour")
                )
                .agg(
                        count("*").alias("transactionCount"),
                        sum("amount").alias("totalAmount"),
                        min("eventTime").alias("firstTransaction"),
                        max("eventTime").alias("lastTransaction")
                )
                .filter(col("transactionCount").geq(3))
                .filter(col("totalAmount").gt(1000.0))
                .orderBy("accountId", "window");
          showAndLog(windowedResults, false);
          // Method 2: Sequential pattern detection using window functions
        logAndPrint("=== SEQUENTIAL PATTERN ANALYSIS ===");
        Dataset<Row> sequential = transactions
                .withColumn("rowNum", 
                        row_number().over(
                                Window.partitionBy("accountId")
                                      .orderBy("eventTime")
                        )
                )
                .withColumn("prev1Amount", 
                        lag("amount", 1).over(
                                Window.partitionBy("accountId")
                                      .orderBy("eventTime")
                        )
                )
                .withColumn("prev2Amount", 
                        lag("amount", 2).over(
                                Window.partitionBy("accountId")
                                      .orderBy("eventTime")
                        )
                )
                .withColumn("prev1Time", 
                        lag("eventTime", 1).over(
                                Window.partitionBy("accountId")
                                      .orderBy("eventTime")
                        )
                )
                .withColumn("prev2Time", 
                        lag("eventTime", 2).over(
                                Window.partitionBy("accountId")
                                      .orderBy("eventTime")
                        )
                )
                .filter(col("rowNum").geq(3))
                .withColumn("timeSpanMinutes", 
                        (col("eventTime").cast("long").minus(col("prev2Time").cast("long"))).divide(60)
                )
                .filter(col("timeSpanMinutes").leq(60)) // Within 1 hour
                .withColumn("threeTransactionTotal", 
                        col("amount").plus(col("prev1Amount")).plus(col("prev2Amount"))
                )
                .filter(col("threeTransactionTotal").gt(1000.0))
                .select(
                        col("accountId"),
                        col("prev2Time").alias("startTime"),
                        col("eventTime").alias("endTime"),
                        col("timeSpanMinutes"),
                        col("prev2Amount").alias("transaction1"),
                        col("prev1Amount").alias("transaction2"),
                        col("amount").alias("transaction3"),
                        col("threeTransactionTotal").alias("totalAmount")
                );
        
        showAndLog(sequential, false);        // Generate final alerts
        logAndPrint("=== FINAL ALERTS ===");
        if (sequential.count() > 0) {
            Dataset<Row> alerts = sequential.select(
                    concat(
                            lit("[FRAUD ALERT] for Account "), 
                            col("accountId"), 
                            lit(": 3 transactions ($"), 
                            format_number(col("transaction1"), 2), lit(", $"),
                            format_number(col("transaction2"), 2), lit(", $"),
                            format_number(col("transaction3"), 2), lit(") "),
                            lit("totaling $"), format_number(col("totalAmount"), 2),
                            lit(" within "), col("timeSpanMinutes"), lit(" minutes")
                    ).alias("Alert")
            );
            showAndLog(alerts, false);
        } else {
            logAndPrint("No suspicious patterns detected in the current dataset.");
        }
          // Summary statistics
        logAndPrint("=== SUMMARY ===");
        logAndPrint("Total transactions: " + transactions.count());
        logAndPrint("Accounts with alerts: " + sequential.select("accountId").distinct().count());
        
        // Close log file
        if (logWriter != null) {
            logWriter.close();
        }
        
        spark.stop();
        logAndPrint(">>> SparkCEPTransactionBatch ending");
    }
}
