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
2025-06-13 12:00:00,acct2,600.00
2025-06-13 12:15:00,acct2,450.00
2025-06-13 12:30:00,acct2,200.00
2025-06-13 14:00:00,acct3,150.00
2025-06-13 14:30:00,acct3,250.00
2025-06-13 15:00:00,acct3,100.00
2025-06-13 16:00:00,acct1,500.00
2025-06-13 16:15:00,acct1,300.00
2025-06-13 16:30:00,acct1,250.00
2025-06-13 18:00:00,acct4,800.00
2025-06-13 18:10:00,acct4,300.00
2025-06-13 18:20:00,acct4,100.00
```

## Comprehensive Logging

Each execution creates a timestamped log file with complete analysis details. Log files capture:

- DataFrame schemas and column information
- All transaction data in readable format
- CEP analysis results with timestamps
- Detailed fraud alerts
- Summary statistics

### Log File Example

```text
2025-06-19T12:19:50.2304692 - === RAW TRANSACTIONS ===
2025-06-19T12:19:50.2304692 - DataFrame Content:
2025-06-19T12:19:50.2304692 - Schema: root
 |-- timestamp: timestamp (nullable = true)
 |-- accountId: string (nullable = true)
 |-- transactionAmount: double (nullable = true)
2025-06-19T12:19:50.2304692 - Columns: timestamp, accountId, transactionAmount
2025-06-19T12:19:50.2304692 - Row 1: 2025-06-13 10:00:00.0 | acct1 | 400.0
2025-06-19T12:19:50.2304692 - Row 2: 2025-06-13 10:15:00.0 | acct1 | 350.0
...
2025-06-19T12:19:50.2304692 - Total rows: 15
```

## Expected Output

### Console and Log File Output

#### 1. Raw Transaction Analysis

```text
=== RAW TRANSACTIONS ===
+-------------------+---------+-----------------+
|          timestamp|accountId|transactionAmount|
+-------------------+---------+-----------------+
|2025-06-13 10:00:00|    acct1|            400.0|
|2025-06-13 10:15:00|    acct1|            350.0|
|2025-06-13 10:45:00|    acct1|            300.0|
|2025-06-13 12:00:00|    acct2|            600.0|
...
```

#### 2. Window-Based Analysis Results

```text
=== WINDOW-BASED ANALYSIS ===
+---------+------------------------------------------+----------------+-----------+
|accountId|window                                    |transactionCount|totalAmount|
+---------+------------------------------------------+----------------+-----------+
|acct1    |{2025-06-13 10:00:00, 2025-06-13 11:00:00}|3               |1050.0     |
|acct1    |{2025-06-13 16:00:00, 2025-06-13 17:00:00}|3               |1050.0     |
|acct2    |{2025-06-13 12:00:00, 2025-06-13 13:00:00}|3               |1250.0     |
|acct4    |{2025-06-13 18:00:00, 2025-06-13 19:00:00}|3               |1200.0     |
+---------+------------------------------------------+----------------+-----------+
```

#### 3. Sequential Pattern Analysis

```text
=== SEQUENTIAL PATTERN ANALYSIS ===
+---------+-------------------+-------------------+---------------+------------+
|accountId|startTime          |endTime            |timeSpanMinutes|totalAmount |
+---------+-------------------+-------------------+---------------+------------+
|acct1    |2025-06-13 10:00:00|2025-06-13 10:45:00|45.0           |1050.0      |
|acct1    |2025-06-13 16:00:00|2025-06-13 16:30:00|30.0           |1050.0      |
|acct2    |2025-06-13 12:00:00|2025-06-13 12:30:00|30.0           |1250.0      |
|acct4    |2025-06-13 18:00:00|2025-06-13 18:20:00|20.0           |1200.0      |
+---------+-------------------+-------------------+---------------+------------+
```

#### 4. Fraud Alerts

```text
=== FINAL ALERTS ===
+-----------------------------------------------------------------------------------------+
|Alert                                                                                    |
+-----------------------------------------------------------------------------------------+
|[FRAUD ALERT] for Account acct1: 3 transactions ($400.00, $350.00, $300.00) totaling  |
|$1,050.00 within 45.0 minutes                                                           |
|[FRAUD ALERT] for Account acct1: 3 transactions ($500.00, $300.00, $250.00) totaling  |
|$1,050.00 within 30.0 minutes                                                           |
|[FRAUD ALERT] for Account acct2: 3 transactions ($600.00, $450.00, $200.00) totaling  |
|$1,250.00 within 30.0 minutes                                                           |
|[FRAUD ALERT] for Account acct4: 3 transactions ($800.00, $300.00, $100.00) totaling  |
|$1,200.00 within 20.0 minutes                                                           |
+-----------------------------------------------------------------------------------------+
```

#### 5. Summary Statistics

```text
=== SUMMARY ===
Total transactions: 15
Accounts with alerts: 3
```

### Pattern Detection Results

- **acct1**: 2 separate fraud patterns detected (10:00-10:45 and 16:00-16:30)
- **acct2**: 1 fraud pattern (12:00-12:30, total: $1,250)
- **acct4**: 1 fraud pattern (18:00-18:20, total: $1,200)  
- **acct3**: No alerts (3 transactions totaling $500 - below $1000 threshold)

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

## Available Scripts

The project includes multiple execution scripts for different use cases:

### Batch Processing Scripts

- **`run-spark-batch.bat`** - Primary batch processing with full CEP analysis
- **`run-batch-test.bat`** - Batch processing with testing options
- **`run-java-direct.bat`** - Direct JAR execution with Java flags

### Streaming Scripts

- **`run-spark-streaming.bat`** - Real-time streaming processing
- **`run-simple-test.bat`** - Simple streaming test

### Testing Scripts

- **`test-minimal-main.bat`** - Minimal functionality test
- **`test-minimal.bat`** - Basic validation test
- **`test-ultimate.bat`** - Comprehensive testing
- **`debug-run.bat`** - Debug mode execution

### Log File Management

Each execution creates timestamped log files:

- **Batch logs**: `spark-cep-batch-YYYY-MM-DD_HH-mm-ss.log`
- **Streaming logs**: `spark-cep-streaming-YYYY-MM-DD_HH-mm-ss.log`
- **Simple logs**: `spark-cep-simple-YYYY-MM-DD_HH-mm-ss.log`
- **Test logs**: `spark-test-YYYY-MM-DD_HH-mm-ss.log`

Log files contain complete execution details including DataFrame schemas, all row data, and analysis results.
