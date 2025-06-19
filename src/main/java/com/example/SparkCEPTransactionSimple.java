package com.example;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.*;

public class SparkCEPTransactionSimple {

    private static final Logger LOG = LoggerFactory.getLogger(SparkCEPTransactionSimple.class);
    
    public static void main(String[] args) {
        
        System.out.println(">>> SparkCEPTransactionSimple start");
        
        try {
            // Create Spark Session with minimal config
            SparkSession spark = SparkSession.builder()
                    .appName("Simple Spark CEP")
                    .master("local[2]")
                    .config("spark.sql.shuffle.partitions", "2")
                    .getOrCreate();
            
            spark.sparkContext().setLogLevel("WARN");
            
            // First try batch processing to verify data loading
            System.out.println("=== TESTING BATCH FIRST ===");
            Dataset<Row> batchData = spark
                    .read()
                    .format("csv")
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .load("transactions.csv");
            
            System.out.println("Batch data loaded successfully:");
            batchData.show();
            batchData.printSchema();
            
            // Now try streaming
            System.out.println("=== STARTING STREAMING ===");
            
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
            
            System.out.println("Streaming started. Waiting for data...");
            System.out.println("Press Ctrl+C to stop");
            
            // Wait for 30 seconds then stop
            Thread.sleep(30000);
            
            query.stop();
            spark.stop();
            
        } catch (Exception e) {
            System.err.println("Error occurred:");
            e.printStackTrace();
        }
        
        System.out.println(">>> SparkCEPTransactionSimple end");
    }
}
