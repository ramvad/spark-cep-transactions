# Spark CEP Transaction Detector

A Complex Event Processing (CEP) application built with Apache Spark that detects suspicious transaction patterns in real-time.

## Business Goal

Detect fraudulent transaction patterns by identifying:
- **3 or more transactions** from the same account
- **Within a 1-hour time window**
- **Total amount exceeding $1000**

## Architecture Migration

This project was migrated from **Apache Flink CEP** to **Apache Spark Structured Streaming** to demonstrate equivalent Complex Event Processing capabilities.

### Key Differences:

| Aspect | Flink CEP | Spark CEP |
|--------|-----------|-----------|
| **Pattern Definition** | Declarative Pattern API | Window functions + SQL |
| **Event Time** | Native watermarks | Structured Streaming watermarks |
| **Stream Processing** | DataStream API | Dataset/DataFrame API |
| **State Management** | Automatic | Windowing + groupBy |
| **Pattern Matching** | Built-in CEP library | Custom logic with window functions |

## Components

### 1. SparkCEPTransactionDetector (Streaming)
- **Real-time processing** using Structured Streaming
- **Two CEP approaches**:
  - Window-based aggregation
  - Sequential pattern detection with lag functions
- **Continuous monitoring** with configurable triggers

### 2. SparkCEPTransactionBatch (Batch)
- **Batch processing** for testing and analysis
- **Same CEP logic** applied to historical data
- **Detailed output** for pattern verification

## How to Run

### Prerequisites
- Java 17+
- Maven 3.6+
- Apache Spark 3.5.0 (included as dependency)

### Batch Processing (Recommended for testing)
```bash
# Windows
run-spark-batch.bat

# Manual execution
mvn clean package
mvn exec:java -Dexec.mainClass="com.example.SparkCEPTransactionBatch"
```

### Streaming Processing
```bash
# Windows  
run-spark-streaming.bat

# Manual execution
mvn clean package
mvn exec:java -Dexec.mainClass="com.example.SparkCEPTransactionDetector"
```

## Sample Data

The `transactions.csv` file contains sample transaction data:

```csv
timestamp,accountId,transactionAmount
2025-06-13 10:00:00,acct1,400.00
2025-06-13 10:15:00,acct1,350.00
2025-06-13 10:45:00,acct1,300.00
...
```

## Expected Output

### Alert Example:
```
🚨 FRAUD ALERT for Account acct1: 3 transactions ($400.00, $350.00, $300.00) 
totaling $1,050.00 within 45 minutes
```

### Pattern Detection:
- **acct1**: 3 transactions totaling $1,050 (ALERT)
- **acct2**: 3 transactions totaling $1,250 (ALERT)  
- **acct4**: 3 transactions totaling $1,200 (ALERT)
- **acct3**: 3 transactions totaling $500 (No alert - below threshold)

## Technical Implementation

### CEP Pattern Logic:
1. **Time Windows**: 1-hour sliding windows with 15-minute intervals
2. **Sequential Detection**: Using SQL window functions (lag, lead)
3. **Aggregation**: Count, sum, and temporal analysis
4. **Watermarking**: Handle late-arriving events (10-minute watermark)

### Spark SQL Features Used:
- `window()` function for time-based grouping
- `lag()` and `row_number()` for sequential analysis
- `watermark()` for event time processing
- Custom aggregations and filtering

## Performance Considerations

- **Local mode**: `master("local[*]")` uses all CPU cores
- **Trigger intervals**: 30 seconds for alerts, 10 seconds for transactions
- **Watermarking**: 10-minute delay for late events
- **Partitioning**: By accountId for optimal pattern detection

## Extensions

### Possible Enhancements:
1. **Kafka Integration**: Real-time data ingestion
2. **Multiple Patterns**: Different fraud detection rules
3. **Machine Learning**: ML-based anomaly detection
4. **Dashboard**: Real-time visualization
5. **Alerting**: Integration with notification systems

## Dependencies

```xml
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-sql_2.12</artifactId>
    <version>3.5.0</version>
</dependency>
```

## Migration Notes

### From Flink to Spark:
- **Pattern API** → **Window Functions**
- **CEP.pattern()** → **Custom SQL logic**
- **PatternStream** → **Structured Streaming**
- **EventTime** → **Watermarking**
- **KeyedStream** → **groupBy**

This demonstrates that complex event processing can be effectively implemented in Spark using its rich SQL and streaming capabilities, even without a dedicated CEP library like Flink's.
