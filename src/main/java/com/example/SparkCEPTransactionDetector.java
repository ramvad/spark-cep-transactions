package com.example;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

public class SparkCEPTransactionDetector {

    private static final Logger LOG = LoggerFactory.getLogger(SparkCEPTransactionDetector.class);
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
    
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        
        // Initialize log file
        try {
            String logFileName = "spark-cep-streaming-" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss")) + ".log";
            logWriter = new PrintWriter(new FileWriter(logFileName, true));
            logAndPrint(">>> SparkCEPTransactionDetector start - Log file: " + logFileName);
        } catch (IOException e) {
            System.err.println("Failed to initialize log file: " + e.getMessage());
            LOG.error("Failed to initialize log file", e);
        }
        
        logAndPrint(">>> SparkCEPTransactionDetector starting...");
        
        // Create Spark Session
        SparkSession spark = SparkSession.builder()
                .appName("Spark CEP Transaction Detector")
                .master("local[*]")
                .config("spark.sql.adaptive.enabled", "false")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
                .getOrCreate();
        
        // Set log level to reduce noise
        spark.sparkContext().setLogLevel("WARN");
        
        logAndPrint("=== STREAMING SETUP ===");
        
        // Define schema for transaction data
        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("timestamp", DataTypes.StringType, false),
                DataTypes.createStructField("accountId", DataTypes.StringType, false),
                DataTypes.createStructField("transactionAmount", DataTypes.DoubleType, false)
        });
        
        // For demonstration, we'll create a streaming source using the rate source
        // This simulates receiving transactions at a controlled rate
        Dataset<Row> rateStream = spark
                .readStream()
                .format("rate")
                .option("rowsPerSecond", "1")
                .option("numPartitions", "1")
                .load();
        
        // Read the static CSV data to simulate a lookup table
        Dataset<Row> staticData = spark
                .read()
                .format("csv")
                .option("header", "true")
                .schema(schema)
                .load("transactions.csv")
                .withColumn("eventTime", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
                .withColumn("amount", col("transactionAmount"))
                .select("accountId", "amount", "eventTime")
                .filter(col("amount").gt(0));
        
        long totalRows = staticData.count();
        logAndPrint("Static data loaded: " + totalRows + " transactions");
        
        // Create a streaming source by cycling through the static data
        // This simulates receiving the transactions over time
        Dataset<Row> transactions = rateStream
                .withColumn("dataIndex", col("value").mod(totalRows))
                .join(
                    staticData.withColumn("rowIndex", monotonically_increasing_id().mod(totalRows)),
                    col("dataIndex").equalTo(col("rowIndex"))
                )
                .select("accountId", "amount", "eventTime")
                .withColumn("streamTime", current_timestamp()); // Add actual stream processing time
        
        logAndPrint("=== SETTING UP CEP STREAMING QUERIES ===");
        
        // CEP Query 1: Window-based aggregation to detect multiple transactions
        Dataset<Row> windowedTransactions = transactions
                .withWatermark("streamTime", "30 seconds")
                .groupBy(
                        col("accountId"),
                        window(col("streamTime"), "2 minutes", "30 seconds") // 2-minute window, sliding every 30 seconds
                )
                .agg(
                        count("*").alias("transactionCount"),
                        sum("amount").alias("totalAmount"),
                        collect_list(col("amount")).alias("amountList"),
                        min("eventTime").alias("firstTransaction"),
                        max("eventTime").alias("lastTransaction")
                );
        
        // Generate alerts for suspicious patterns (adjusted thresholds for demo)
        Dataset<Row> alerts = windowedTransactions
                .filter(col("transactionCount").geq(2)) // At least 2 transactions
                .filter(col("totalAmount").gt(500.0))   // Total amount > $500
                .select(
                        col("accountId"),
                        col("transactionCount"),
                        col("totalAmount"),
                        col("window.start").alias("windowStart"),
                        col("window.end").alias("windowEnd"),
                        col("amountList")
                )
                .withColumn("alert", 
                        concat(
                                lit("[STREAMING ALERT] Account "), 
                                col("accountId"), 
                                lit(" had "), 
                                col("transactionCount"), 
                                lit(" transactions totaling $"), 
                                format_number(col("totalAmount"), 2),
                                lit(" in window "), 
                                col("windowStart"), 
                                lit(" to "), 
                                col("windowEnd")
                        )
                );
        
        // Start streaming query to show all processed transactions
        StreamingQuery transactionQuery = transactions
                .select("streamTime", "accountId", "amount", "eventTime")
                .writeStream()
                .outputMode("append")
                .format("console")
                .option("truncate", false)
                .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("5 seconds"))
                .queryName("ProcessedTransactions")
                .start();
        
        // Start streaming query for windowed alerts
        StreamingQuery alertQuery = alerts
                .select("alert", "accountId", "transactionCount", "totalAmount", "windowStart", "windowEnd")
                .writeStream()
                .outputMode("update")
                .format("console")
                .option("truncate", false)
                .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("10 seconds"))
                .queryName("FraudAlerts")
                .start();
        
        logAndPrint("=== STREAMING QUERIES STARTED ===");
        logAndPrint("Spark CEP Transaction Detector is running...");
        logAndPrint("Monitoring for suspicious transaction patterns:");
        logAndPrint("- 2+ transactions within 2-minute sliding windows");
        logAndPrint("- Total amount > $500");
        logAndPrint("- Processing transactions every 5-10 seconds");
        logAndPrint("- Rate: 1 transaction per second");
        logAndPrint("");
        logAndPrint("Watch the console for:");
        logAndPrint("1. Processed transactions (every 5 seconds)");
        logAndPrint("2. Fraud alerts (every 10 seconds when detected)");
        logAndPrint("");
        logAndPrint("Press Ctrl+C to stop...");
        
        try {
            // Wait for the main alert query to finish (runs until manually stopped)
            alertQuery.awaitTermination();
        } catch (Exception e) {
            LOG.error("Error in streaming queries", e);
            logAndPrint("Streaming error: " + e.getMessage());
        } finally {
            // Stop all queries gracefully
            try {
                if (transactionQuery.isActive()) {
                    transactionQuery.stop();
                }
                if (alertQuery.isActive()) {
                    alertQuery.stop();
                }
            } catch (Exception e) {
                LOG.error("Error stopping queries", e);
            }
            
            // Close log file
            if (logWriter != null) {
                logWriter.close();
            }
            
            spark.stop();
            logAndPrint(">>> SparkCEPTransactionDetector ending");
        }
    }
}
