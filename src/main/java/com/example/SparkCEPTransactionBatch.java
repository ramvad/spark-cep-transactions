package com.example;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.*;

public class SparkCEPTransactionBatch {

    private static final Logger LOG = LoggerFactory.getLogger(SparkCEPTransactionBatch.class);
    
    public static void main(String[] args) {
        
        System.out.println(">>> SparkCEPTransactionBatch start");
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
                .load("transactions.csv");
        
        System.out.println("=== RAW TRANSACTIONS ===");
        rawTransactions.show();
        
        // Convert timestamp and prepare data
        Dataset<Row> transactions = rawTransactions
                .withColumn("eventTime", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
                .withColumn("amount", col("transactionAmount"))
                .select("accountId", "amount", "eventTime")
                .filter(col("amount").gt(0))
                .orderBy("accountId", "eventTime");
        
        System.out.println("=== PROCESSED TRANSACTIONS ===");
        transactions.show();
        
        // Method 1: Window-based analysis
        System.out.println("=== WINDOW-BASED ANALYSIS ===");
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
        
        windowedResults.show(false);
        
        // Method 2: Sequential pattern detection using window functions
        System.out.println("=== SEQUENTIAL PATTERN ANALYSIS ===");
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
        
        sequential.show(false);
        
        // Generate final alerts
        System.out.println("=== FINAL ALERTS ===");
        if (sequential.count() > 0) {
            sequential.select(
                    concat(
                            lit("🚨 FRAUD ALERT for Account "), 
                            col("accountId"), 
                            lit(": 3 transactions ($"), 
                            format_number(col("transaction1"), 2), lit(", $"),
                            format_number(col("transaction2"), 2), lit(", $"),
                            format_number(col("transaction3"), 2), lit(") "),
                            lit("totaling $"), format_number(col("totalAmount"), 2),
                            lit(" within "), col("timeSpanMinutes"), lit(" minutes")
                    ).alias("Alert")
            ).show(false);
        } else {
            System.out.println("No suspicious patterns detected in the current dataset.");
        }
        
        // Summary statistics
        System.out.println("=== SUMMARY ===");
        System.out.println("Total transactions: " + transactions.count());
        System.out.println("Accounts with alerts: " + sequential.select("accountId").distinct().count());
        
        spark.stop();
        System.out.println(">>> SparkCEPTransactionBatch ending");
    }
}
