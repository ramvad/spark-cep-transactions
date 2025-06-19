package com.example;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

public class SparkCEPTransactionDetector {

    private static final Logger LOG = LoggerFactory.getLogger(SparkCEPTransactionDetector.class);
    
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        
        System.out.println(">>> SparkCEPTransactionDetector start");
        LOG.info(">>> SparkCEPTransactionDetector starting...");
        
        // Create Spark Session
        SparkSession spark = SparkSession.builder()
                .appName("Spark CEP Transaction Detector")
                .master("local[*]")
                .config("spark.sql.adaptive.enabled", "false")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
                .getOrCreate();
        
        // Set log level to reduce noise
        spark.sparkContext().setLogLevel("WARN");
        
        // Define schema for transaction data
        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("timestamp", DataTypes.StringType, false),
                DataTypes.createStructField("accountId", DataTypes.StringType, false),
                DataTypes.createStructField("transactionAmount", DataTypes.DoubleType, false)
        });
          // Read CSV file as a streaming source (simulating real-time data)
        Dataset<Row> rawTransactions = spark
                .readStream()
                .format("csv")
                .option("header", "true")
                .schema(schema)
                .option("path", "transactions.csv")
                .load();
        
        // Convert timestamp string to timestamp type and add event time
        Dataset<Row> transactions = rawTransactions
                .withColumn("eventTime", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
                .withColumn("amount", col("transactionAmount"))
                .select("accountId", "amount", "eventTime")
                .filter(col("amount").gt(0)); // Only positive amounts
        
        // Complex Event Processing: Detect patterns of 3+ transactions within 1 hour
        // Using window functions to detect consecutive transactions
        Dataset<Row> windowedTransactions = transactions
                .withWatermark("eventTime", "10 minutes")
                .groupBy(
                        col("accountId"),
                        window(col("eventTime"), "1 hour", "15 minutes") // 1-hour window, sliding every 15 minutes
                )
                .agg(
                        count("*").alias("transactionCount"),
                        sum("amount").alias("totalAmount"),
                        collect_list(struct(col("eventTime"), col("amount"))).alias("transactionList"),
                        min("eventTime").alias("firstTransaction"),
                        max("eventTime").alias("lastTransaction")
                );
        
        // Generate alerts for suspicious patterns
        Dataset<Row> alerts = windowedTransactions
                .filter(col("transactionCount").geq(3)) // At least 3 transactions
                .filter(col("totalAmount").gt(1000.0))  // Total amount > $1000
                .select(
                        col("accountId"),
                        col("totalAmount"),
                        col("transactionCount"),
                        col("window.start").alias("windowStart"),
                        col("window.end").alias("windowEnd"),
                        col("firstTransaction"),
                        col("lastTransaction")
                )
                .withColumn("alert", 
                        concat(
                                lit("ALERT: Account "), 
                                col("accountId"), 
                                lit(" had "), 
                                col("transactionCount"), 
                                lit(" transactions totaling $"), 
                                format_number(col("totalAmount"), 2),
                                lit(" between "), 
                                col("firstTransaction"), 
                                lit(" and "), 
                                col("lastTransaction")
                        )
                );
        
        // Alternative approach: Detect sequential patterns using window functions
        Dataset<Row> sequentialPatterns = transactions
                .withWatermark("eventTime", "10 minutes")
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
                .filter(col("rowNum").geq(3)) // At least 3 transactions
                .filter(
                        // Check if all 3 transactions are within 1 hour
                        col("eventTime").minus(col("prev2Time"))
                                .divide(3600).lt(1.0) // Less than 1 hour in seconds
                )
                .withColumn("threeTransactionTotal", 
                        col("amount").plus(col("prev1Amount")).plus(col("prev2Amount"))
                )
                .filter(col("threeTransactionTotal").gt(1000.0))
                .select(
                        col("accountId"),
                        col("threeTransactionTotal").alias("totalAmount"),
                        col("prev2Time").alias("firstTransactionTime"),
                        col("eventTime").alias("lastTransactionTime")
                )
                .withColumn("sequentialAlert", 
                        concat(
                                lit("SEQUENTIAL ALERT: Account "), 
                                col("accountId"), 
                                lit(" had 3 consecutive transactions totaling $"), 
                                format_number(col("totalAmount"), 2),
                                lit(" from "), 
                                col("firstTransactionTime"), 
                                lit(" to "), 
                                col("lastTransactionTime")
                        )
                );
        
        // Start streaming query for windowed alerts
        StreamingQuery windowQuery = alerts
                .writeStream()
                .outputMode("update")
                .format("console")
                .option("truncate", false)
                .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("30 seconds"))
                .queryName("WindowedAlerts")
                .start();
        
        // Start streaming query for sequential pattern alerts
        StreamingQuery sequentialQuery = sequentialPatterns
                .writeStream()
                .outputMode("append")
                .format("console")
                .option("truncate", false)
                .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("30 seconds"))
                .queryName("SequentialAlerts")
                .start();
        
        // Also output all processed transactions for monitoring
        StreamingQuery transactionQuery = transactions
                .writeStream()
                .outputMode("append")
                .format("console")
                .option("truncate", false)
                .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("10 seconds"))
                .queryName("ProcessedTransactions")
                .start();
        
        System.out.println("Spark CEP Transaction Detector is running...");
        System.out.println("Monitoring for suspicious transaction patterns:");
        System.out.println("- 3+ transactions within 1 hour");
        System.out.println("- Total amount > $1000");
        System.out.println("Press Ctrl+C to stop...");
        
        try {
            // Wait for queries to finish (they won't in streaming mode)
            windowQuery.awaitTermination();
            sequentialQuery.awaitTermination();
            transactionQuery.awaitTermination();
        } catch (Exception e) {
            LOG.error("Error in streaming queries", e);
        } finally {
            spark.stop();
        }
        
        System.out.println(">>> SparkCEPTransactionDetector ending");
    }
}
