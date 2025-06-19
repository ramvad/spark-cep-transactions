package com.example;

import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.spark.sql.functions.*;

/**
 * Enhanced Spark CEP Transaction Detector with Scheduling Capabilities
 * 
 * Features:
 * - Batch processing with configurable intervals
 * - Incremental processing (only new data since last run)
 * - Comprehensive logging and monitoring
 * - Easy configuration for different deployment scenarios
 */
public class SparkCEPTransactionScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(SparkCEPTransactionScheduler.class);
    private static PrintWriter logWriter;
    private static LocalDateTime lastProcessedTime = LocalDateTime.now().minusDays(1); // Start from yesterday
    
    // Configuration
    private static final int PROCESSING_INTERVAL_MINUTES = 15; // Run every 15 minutes
    private static final String DATA_FILE = "transactions.csv";
    private static final boolean CONTINUOUS_MODE = false; // Set to true for scheduled runs
    
    private static void logAndPrint(String message) {
        String timestampedMessage = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + " - " + message;
        System.out.println(timestampedMessage);
        LOG.info(message);
        if (logWriter != null) {
            logWriter.println(timestampedMessage);
            logWriter.flush();
        }
    }
    
    public static void main(String[] args) {
        // Initialize logging
        initializeLogging();
        
        if (CONTINUOUS_MODE) {
            runScheduledProcessing();
        } else {
            runSingleBatch();
        }
    }
    
    private static void initializeLogging() {
        try {
            String logFileName = "spark-cep-scheduler-" + 
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss")) + ".log";
            logWriter = new PrintWriter(new FileWriter(logFileName, true));
            logAndPrint("=== Spark CEP Transaction Scheduler Started ===");
            logAndPrint("Log file: " + logFileName);
            logAndPrint("Processing interval: " + PROCESSING_INTERVAL_MINUTES + " minutes");
            logAndPrint("Continuous mode: " + CONTINUOUS_MODE);
        } catch (IOException e) {
            System.err.println("Failed to initialize log file: " + e.getMessage());
        }
    }
    
    private static void runSingleBatch() {
        logAndPrint("Running single batch analysis...");
        processTransactions();
        logAndPrint("Single batch completed.");
        if (logWriter != null) {
            logWriter.close();
        }
    }
    
    private static void runScheduledProcessing() {
        logAndPrint("Starting scheduled processing mode...");
        
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        
        // Run immediately, then every PROCESSING_INTERVAL_MINUTES
        scheduler.scheduleAtFixedRate(() -> {
            try {
                logAndPrint("=== Scheduled Processing Run ===");
                processTransactions();
                logAndPrint("=== Scheduled Processing Complete ===");
            } catch (Exception e) {
                logAndPrint("ERROR in scheduled processing: " + e.getMessage());
                LOG.error("Scheduled processing error", e);
            }
        }, 0, PROCESSING_INTERVAL_MINUTES, TimeUnit.MINUTES);
        
        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logAndPrint("Shutting down scheduler...");
            scheduler.shutdown();
            if (logWriter != null) {
                logWriter.close();
            }
        }));
        
        logAndPrint("Scheduler started. Press Ctrl+C to stop.");
        
        // Keep the main thread alive
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            logAndPrint("Main thread interrupted. Shutting down...");
            scheduler.shutdown();
        }
    }
    
    private static void processTransactions() {
        SparkSession spark = null;
        try {
            // Create Spark session with optimized batch settings
            spark = SparkSession.builder()
                    .appName("Spark CEP Transaction Scheduler")
                    .master("local[*]")
                    .config("spark.sql.adaptive.enabled", "true")
                    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                    .config("spark.sql.adaptive.skewJoin.enabled", "true")
                    // Windows compatibility
                    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
                    .config("spark.sql.warehouse.dir", "file:///C:/tmp/spark-warehouse")
                    .config("spark.hadoop.io.native.lib.available", "false")
                    .getOrCreate();
            
            spark.sparkContext().setLogLevel("WARN");
            
            logAndPrint("Loading transaction data from: " + DATA_FILE);
            
            // Read transaction data
            Dataset<Row> transactions = spark.read()
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .csv(DATA_FILE)
                    .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
                    .withColumnRenamed("transactionAmount", "amount");
            
            // Filter for new transactions since last run (in scheduled mode)
            if (CONTINUOUS_MODE) {
                String lastProcessedStr = lastProcessedTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                logAndPrint("Processing transactions since: " + lastProcessedStr);
                transactions = transactions.filter(col("timestamp").gt(lit(lastProcessedStr)));
            }
            
            long transactionCount = transactions.count();
            logAndPrint("Processing " + transactionCount + " transactions");
            
            if (transactionCount == 0) {
                logAndPrint("No new transactions to process");
                return;
            }
            
            // Show sample data
            logAndPrint("=== SAMPLE TRANSACTION DATA ===");
            transactions.show(5, false);
            
            // CEP Analysis: Find suspicious patterns
            Dataset<Row> suspiciousPatterns = transactions
                    .withWatermark("timestamp", "1 hour")
                    .groupBy(
                            col("accountId"),
                            window(col("timestamp"), "1 hour")
                    )
                    .agg(
                            count("*").alias("transactionCount"),
                            sum("amount").alias("totalAmount"),
                            collect_list("amount").alias("amounts"),
                            min("timestamp").alias("firstTransaction"),
                            max("timestamp").alias("lastTransaction")
                    )
                    .filter(col("transactionCount").geq(3).and(col("totalAmount").gt(1000)))
                    .orderBy(desc("totalAmount"));
            
            // Display results
            long alertCount = suspiciousPatterns.count();
            logAndPrint("=== CEP ANALYSIS RESULTS ===");
            logAndPrint("Found " + alertCount + " suspicious patterns");
            
            if (alertCount > 0) {
                logAndPrint("=== FRAUD ALERTS ===");
                suspiciousPatterns.show(20, false);
                  // Log detailed alerts
                Row[] alerts = (Row[]) suspiciousPatterns.collect();
                for (Row alert : alerts) {
                    String accountId = alert.getAs("accountId");
                    Long alertTransactionCount = alert.getAs("transactionCount");
                    Double totalAmount = alert.getAs("totalAmount");
                    
                    logAndPrint("🚨 FRAUD ALERT: Account " + accountId + 
                               " - " + alertTransactionCount + " transactions totaling $" + 
                               String.format("%.2f", totalAmount));
                }
            } else {
                logAndPrint("✅ No suspicious patterns detected");
            }
            
            // Update last processed time for next run
            lastProcessedTime = LocalDateTime.now();
            logAndPrint("Updated last processed time to: " + lastProcessedTime);
            
        } catch (Exception e) {
            logAndPrint("ERROR during processing: " + e.getMessage());
            LOG.error("Processing error", e);
        } finally {
            if (spark != null) {
                spark.stop();
            }
        }
    }
}
