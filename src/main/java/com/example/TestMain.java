package com.example;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import static org.apache.spark.sql.functions.*;

public class TestMain {
    
    public static void main(String[] args) {
        
        System.out.println("=== MINIMAL SPARK TEST ===");
        
        try {
            // Create the simplest possible Spark session
            SparkSession spark = SparkSession.builder()
                    .appName("Minimal Test")
                    .master("local[1]")
                    .config("spark.ui.enabled", "false")
                    .config("spark.sql.adaptive.enabled", "false")
                    .getOrCreate();
            
            spark.sparkContext().setLogLevel("ERROR");
            
            System.out.println("✅ Spark session created successfully!");
            
            // Test basic functionality
            Dataset<Row> df = spark.read()
                    .format("csv")
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .load("transactions.csv");
            
            System.out.println("✅ CSV file loaded successfully!");
            System.out.println("Row count: " + df.count());
            
            df.show(5);
            
            // Test basic CEP logic
            Dataset<Row> processed = df
                    .withColumn("eventTime", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
                    .withColumn("amount", col("transactionAmount"))
                    .select("accountId", "amount", "eventTime")
                    .orderBy("accountId", "eventTime");
            
            System.out.println("✅ Data processing successful!");
            processed.show();
            
            // Test window function
            Dataset<Row> windowed = processed
                    .withColumn("rowNum", 
                            row_number().over(
                                    Window.partitionBy("accountId")
                                          .orderBy("eventTime")
                            )
                    );
            
            System.out.println("✅ Window functions working!");
            windowed.show();
            
            spark.stop();
            System.out.println("✅ ALL TESTS PASSED!");
            
        } catch (Exception e) {
            System.err.println("❌ ERROR: " + e.getMessage());
            e.printStackTrace();
        }
    }
}

