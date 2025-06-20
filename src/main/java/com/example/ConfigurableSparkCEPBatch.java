package com.example;

import com.example.config.FraudPatternConfig;
import com.example.config.FraudPatternConfig.FraudPattern;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

public class ConfigurableSparkCEPBatch {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigurableSparkCEPBatch.class);
    private static PrintWriter logWriter;
    private static FraudPatternConfig patternConfig;
    
    public static void main(String[] args) {
        String configPath = args.length > 0 ? args[0] : "fraud-patterns-config.json";
        
        // Initialize configuration
        patternConfig = new FraudPatternConfig(configPath);
        
        SparkSession spark = SparkSession.builder()
                .appName("Configurable Spark CEP Transaction Batch")
                .master("local[*]")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .getOrCreate();

        try {
            // Setup logging
            setupLogging();
            
            logAndPrint("=== Configurable Spark CEP Transaction Batch Started ===");
            logAndPrint("Configuration version: " + patternConfig.getConfigVersion());
            logAndPrint("Last updated: " + patternConfig.getLastUpdated());
            
            // Load data configuration
            Map<String, String> dataConfig = patternConfig.getDataConfiguration();
            String inputPath = dataConfig.get("inputPath");
            String outputPath = dataConfig.get("outputPath");
            
            // Load and process transactions
            Dataset<Row> transactions = loadTransactions(spark, inputPath);
            
            // Apply all enabled fraud patterns
            Dataset<Row> allAlerts = applyConfigurableFraudPatterns(transactions);
            
            // Process alerts
            processAlerts(allAlerts);
            
            // Save results
            saveResults(allAlerts, outputPath);
            
            logAndPrint("=== Configurable Spark CEP Transaction Batch Completed ===");
            
        } catch (Exception e) {
            LOG.error("Error in Configurable Spark CEP batch processing", e);
            logAndPrint("ERROR: " + e.getMessage());
        } finally {
            if (logWriter != null) {
                logWriter.close();
            }
            spark.stop();
        }
    }
    
    /**
     * Apply all enabled fraud patterns from configuration
     */
    private static Dataset<Row> applyConfigurableFraudPatterns(Dataset<Row> transactions) {
        logAndPrint("Applying configurable fraud patterns...");
        
        // Check for configuration updates
        patternConfig.checkAndReload();
        
        // Get enabled patterns
        List<FraudPattern> enabledPatterns = patternConfig.getEnabledPatterns();
        logAndPrint("Found " + enabledPatterns.size() + " enabled fraud patterns");
        
        Dataset<Row> allAlerts = null;
        
        for (FraudPattern pattern : enabledPatterns) {
            logAndPrint("Applying pattern: " + pattern.getName() + " (ID: " + pattern.getId() + ")");
            
            try {
                Dataset<Row> patternAlerts = applyPattern(transactions, pattern);
                
                if (allAlerts == null) {
                    allAlerts = patternAlerts;
                } else {
                    allAlerts = allAlerts.union(patternAlerts);
                }
                
                long alertCount = patternAlerts.count();
                logAndPrint("Pattern " + pattern.getId() + " generated " + alertCount + " alerts");
                
            } catch (Exception e) {
                LOG.error("Error applying pattern: " + pattern.getId(), e);
                logAndPrint("ERROR in pattern " + pattern.getId() + ": " + e.getMessage());
            }
        }
        
        return allAlerts != null ? allAlerts.distinct() : transactions.limit(0);
    }
    
    /**
     * Apply a specific fraud pattern
     */
    private static Dataset<Row> applyPattern(Dataset<Row> transactions, FraudPattern pattern) {
        Map<String, Object> params = pattern.getParameters();
        
        switch (pattern.getId()) {
            case "HIGH_FREQUENCY_PATTERN":
                return applyHighFrequencyPattern(transactions, pattern);
                
            case "SEQUENTIAL_LARGE_AMOUNTS":
                return applySequentialLargeAmountsPattern(transactions, pattern);
                
            case "UNUSUAL_AMOUNT_THRESHOLD":
                return applyUnusualAmountPattern(transactions, pattern);
                
            case "VELOCITY_PATTERN":
                return applyVelocityPattern(transactions, pattern);
                
            case "MERCHANT_RISK_PATTERN":
                return applyMerchantRiskPattern(transactions, pattern);
                
            default:
                // Generic SQL-based pattern application
                return applyGenericSQLPattern(transactions, pattern);
        }
    }
      /**
     * Apply high frequency pattern using configuration
     */
    private static Dataset<Row> applyHighFrequencyPattern(Dataset<Row> transactions, FraudPattern pattern) {
        Map<String, Object> params = pattern.getParameters();
        
        int windowMinutes = (Integer) params.get("windowSizeMinutes");
        int minTransactionCount = (Integer) params.get("minimumTransactionCount");
        double minTotalAmount = (Double) params.get("minimumTotalAmount");
        
        WindowSpec window = Window.partitionBy("accountId")
                .orderBy("eventTime")
                .rangeBetween(-windowMinutes * 60, 0);
        
        Dataset<Row> windowed = transactions
                .withColumn("transactionCount", count("*").over(window))
                .withColumn("totalAmount", sum("amount").over(window))
                .filter(col("transactionCount").geq(minTransactionCount))
                .filter(col("totalAmount").gt(minTotalAmount));
        
        return windowed
                .withColumn("patternId", lit(pattern.getId()))
                .withColumn("patternName", lit(pattern.getName()))
                .withColumn("alertLevel", lit(params.get("alertLevel").toString()))
                .withColumn("alertMessage", 
                    format_string(pattern.getAlertMessage().replace("${transactionCount}", "%d")
                                                          .replace("${totalAmount}", "%.2f")
                                                          .replace("${windowSizeMinutes}", "%d"),
                        col("transactionCount"), col("totalAmount"), lit(windowMinutes)))
                .withColumn("detectionTime", current_timestamp());
    }
    
    /**
     * Apply sequential large amounts pattern
     */
    private static Dataset<Row> applySequentialLargeAmountsPattern(Dataset<Row> transactions, FraudPattern pattern) {
        Map<String, Object> params = pattern.getParameters();
        
        int minSequenceLength = (Integer) params.get("minimumSequenceLength");
        int timeWindowHours = (Integer) params.get("timeWindowHours");
        double minTotalAmount = (Double) params.get("minimumTotalAmount");
        
        WindowSpec sequenceWindow = Window.partitionBy("accountId").orderBy("eventTime");
        
        Dataset<Row> sequential = transactions
                .withColumn("rowNum", row_number().over(sequenceWindow))
                .withColumn("prev1Amount", lag("amount", 1).over(sequenceWindow))
                .withColumn("prev2Amount", lag("amount", 2).over(sequenceWindow))
                .withColumn("threeTransactionTotal", 
                    col("amount").plus(coalesce(col("prev1Amount"), lit(0.0)))
                                 .plus(coalesce(col("prev2Amount"), lit(0.0))))
                .withColumn("timeSpanMinutes",
                    col("eventTime").minus(lag("eventTime", 2).over(sequenceWindow)).divide(60))
                .filter(col("rowNum").geq(minSequenceLength))
                .filter(col("timeSpanMinutes").leq(timeWindowHours * 60))
                .filter(col("threeTransactionTotal").gt(minTotalAmount));
        
        return sequential
                .withColumn("patternId", lit(pattern.getId()))
                .withColumn("patternName", lit(pattern.getName()))
                .withColumn("alertLevel", lit(params.get("alertLevel").toString()))
                .withColumn("alertMessage", 
                    format_string(pattern.getAlertMessage().replace("${minimumSequenceLength}", "%d")
                                                          .replace("${threeTransactionTotal}", "%.2f"),
                        lit(minSequenceLength), col("threeTransactionTotal")))
                .withColumn("detectionTime", current_timestamp());
    }
    
    /**
     * Apply unusual amount threshold pattern
     */
    private static Dataset<Row> applyUnusualAmountPattern(Dataset<Row> transactions, FraudPattern pattern) {
        Map<String, Object> params = pattern.getParameters();
        double threshold = (Double) params.get("largeAmountThreshold");
        
        Dataset<Row> largeAmounts = transactions
                .filter(col("amount").gt(threshold));
        
        return largeAmounts
                .withColumn("patternId", lit(pattern.getId()))
                .withColumn("patternName", lit(pattern.getName()))
                .withColumn("alertLevel", lit(params.get("alertLevel").toString()))
                .withColumn("alertMessage", 
                    format_string(pattern.getAlertMessage().replace("${amount}", "%.2f"),
                        col("amount")))
                .withColumn("detectionTime", current_timestamp());
    }
    
    /**
     * Apply velocity pattern
     */
    private static Dataset<Row> applyVelocityPattern(Dataset<Row> transactions, FraudPattern pattern) {
        Map<String, Object> params = pattern.getParameters();
        int maxTimeMinutes = (Integer) params.get("maxTimeBetweenTransactionsMinutes");
        
        WindowSpec velocityWindow = Window.partitionBy("accountId").orderBy("eventTime");
        
        Dataset<Row> velocity = transactions
                .withColumn("timeBetweenTransactions",
                    col("eventTime").minus(lag("eventTime", 1).over(velocityWindow)).divide(60))
                .filter(col("timeBetweenTransactions").leq(maxTimeMinutes))
                .filter(col("timeBetweenTransactions").isNotNull());
        
        return velocity
                .withColumn("patternId", lit(pattern.getId()))
                .withColumn("patternName", lit(pattern.getName()))
                .withColumn("alertLevel", lit(params.get("alertLevel").toString()))
                .withColumn("alertMessage", lit(pattern.getAlertMessage()))
                .withColumn("detectionTime", current_timestamp());
    }
      /**
     * Apply merchant risk pattern
     */
    private static Dataset<Row> applyMerchantRiskPattern(Dataset<Row> transactions, FraudPattern pattern) {
        Map<String, Object> params = pattern.getParameters();
        
        @SuppressWarnings("unchecked")
        List<String> riskCategories = (List<String>) params.get("riskMerchantCategories");
        double amountThreshold = (Double) params.get("amountThreshold");
        
        Dataset<Row> riskMerchants = transactions
                .filter(col("merchantCategory").isin(riskCategories.toArray()))
                .filter(col("amount").gt(amountThreshold));
        
        return riskMerchants
                .withColumn("patternId", lit(pattern.getId()))
                .withColumn("patternName", lit(pattern.getName()))
                .withColumn("alertLevel", lit(params.get("alertLevel").toString()))
                .withColumn("alertMessage", 
                    format_string(pattern.getAlertMessage().replace("${merchantCategory}", "%s")
                                                          .replace("${amount}", "%.2f"),
                        col("merchantCategory"), col("amount")))
                .withColumn("detectionTime", current_timestamp());
    }
    
    /**
     * Apply generic SQL-based pattern
     */
    private static Dataset<Row> applyGenericSQLPattern(Dataset<Row> transactions, FraudPattern pattern) {
        // Create temporary view for SQL queries
        transactions.createOrReplaceTempView("transactions");
        
        String sqlCondition = pattern.getSqlCondition();
        String query = "SELECT *, '" + pattern.getId() + "' as patternId, '" + 
                      pattern.getName() + "' as patternName FROM transactions WHERE " + sqlCondition;
        
        SparkSession spark = transactions.sparkSession();
        Dataset<Row> result = spark.sql(query);
        
        return result
                .withColumn("alertLevel", lit(pattern.getParameters().get("alertLevel").toString()))
                .withColumn("alertMessage", lit(pattern.getAlertMessage()))
                .withColumn("detectionTime", current_timestamp());
    }
    
    /**
     * Load transactions with data configuration
     */
    private static Dataset<Row> loadTransactions(SparkSession spark, String inputPath) {
        logAndPrint("Loading transactions from: " + inputPath);
        
        Dataset<Row> transactions = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(inputPath)
                .filter(col("amount").gt(0))
                .withColumn("eventTime", unix_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"));
        
        long totalTransactions = transactions.count();
        logAndPrint("Loaded " + totalTransactions + " transactions");
        
        return transactions;
    }
    
    /**
     * Process alerts with configuration
     */
    private static void processAlerts(Dataset<Row> alerts) {
        long alertCount = alerts.count();
        logAndPrint("Total fraud alerts detected: " + alertCount);
        
        if (alertCount > 0) {
            logAndPrint("Alert breakdown by pattern:");
            alerts.groupBy("patternId", "patternName", "alertLevel")
                  .count()
                  .orderBy(desc("count"))
                  .show(false);
            
            // Show sample alerts
            logAndPrint("Sample fraud alerts:");
            showAndLog(alerts.select("accountId", "amount", "patternName", "alertLevel", "alertMessage")
                            .orderBy(desc("amount"))
                            .limit(10), false);
        }
    }
    
    /**
     * Save results with configuration
     */
    private static void saveResults(Dataset<Row> alerts, String outputPath) {
        if (alerts.count() > 0) {
            logAndPrint("Saving fraud alerts to: " + outputPath);
            
            alerts.coalesce(1)
                  .write()
                  .mode(SaveMode.Overwrite)
                  .option("header", "true")
                  .csv(outputPath);
            
            logAndPrint("Fraud alerts saved successfully");
        }
    }
    
    // Helper methods (same as original implementation)
    private static void setupLogging() {
        try {
            String logFileName = "configurable-spark-cep-batch-" + 
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss")) + ".log";
            logWriter = new PrintWriter(new FileWriter(logFileName, true));
            logAndPrint("Log file: " + logFileName);
        } catch (IOException e) {
            LOG.error("Failed to setup log file", e);
        }
    }
    
    private static void logAndPrint(String message) {
        System.out.println(message);
        LOG.info(message);
        if (logWriter != null) {
            logWriter.println(LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + " - " + message);
            logWriter.flush();
        }
    }
    
    private static void showAndLog(Dataset<Row> df, boolean truncate) {
        df.show(20, truncate);
        
        if (logWriter != null) {
            try {
                String timestamp = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                logWriter.println(timestamp + " - DataFrame Content:");
                logWriter.println(timestamp + " - Schema: " + df.schema().treeString());
                
                Row[] rows = (Row[]) df.collect();
                if (rows.length > 0) {
                    String[] columnNames = df.columns();
                    logWriter.println(timestamp + " - Columns: " + String.join(", ", columnNames));
                    
                    for (int i = 0; i < Math.min(rows.length, 10); i++) {
                        logWriter.println(timestamp + " - Row " + i + ": " + rows[i].toString());
                    }
                }
                logWriter.flush();
            } catch (Exception e) {
                LOG.error("Error logging DataFrame content", e);
            }
        }
    }
}
