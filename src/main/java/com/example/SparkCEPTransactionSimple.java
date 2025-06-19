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

import static org.apache.spark.sql.functions.*;

public class SparkCEPTransactionSimple {

    private static final Logger LOG = LoggerFactory.getLogger(SparkCEPTransactionSimple.class);
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
    
    public static void main(String[] args) {
        
        // Initialize log file
        try {
            String logFileName = "spark-cep-simple-" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss")) + ".log";
            logWriter = new PrintWriter(new FileWriter(logFileName, true));
            logAndPrint(">>> SparkCEPTransactionSimple start - Log file: " + logFileName);
        } catch (IOException e) {
            System.err.println("Failed to initialize log file: " + e.getMessage());
            LOG.error("Failed to initialize log file", e);
        }
        
        try {
            // Create Spark Session with minimal config
            SparkSession spark = SparkSession.builder()
                    .appName("Simple Spark CEP")
                    .master("local[2]")
                    .config("spark.sql.shuffle.partitions", "2")
                    .getOrCreate();
            
            spark.sparkContext().setLogLevel("WARN");
            
            // First try batch processing to verify data loading
            logAndPrint("=== TESTING BATCH FIRST ===");
            Dataset<Row> batchData = spark
                    .read()
                    .format("csv")
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .load("transactions.csv");
            
            logAndPrint("Batch data loaded successfully:");
            batchData.show();
            batchData.printSchema();
            
            // Now try streaming
            logAndPrint("=== STARTING STREAMING ===");
            
            StructType schema = new StructType(new StructField[]{
                    DataTypes.createStructField("timestamp", DataTypes.StringType, false),
                    DataTypes.createStructField("accountId", DataTypes.StringType, false),
                    DataTypes.createStructField("transactionAmount", DataTypes.DoubleType, false)
            });
            
            Dataset<Row> streamingData = spark
                    .readStream()
                    .format("csv")
                    .option("header", "true")
                    .schema(schema)
                    .load("transactions.csv");
            
            // Simple processing - just show the data
            StreamingQuery query = streamingData
                    .writeStream()
                    .outputMode("append")
                    .format("console")
                    .option("truncate", false)
                    .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("5 seconds"))
                    .start();
            
            logAndPrint("Streaming started. Waiting for data...");
            logAndPrint("Press Ctrl+C to stop");
            
            // Wait for 30 seconds then stop
            Thread.sleep(30000);
            
            query.stop();
            spark.stop();
            
        } catch (Exception e) {
            System.err.println("Error occurred:");
            e.printStackTrace();
        } finally {
            // Close log file
            if (logWriter != null) {
                logWriter.close();
            }
        }
        
        logAndPrint(">>> SparkCEPTransactionSimple end");
    }
}
