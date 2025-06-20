package com.example.flink;

import com.example.config.FraudPatternConfig;
import com.example.config.FraudPatternConfig.FraudPattern;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Configurable Flink CEP implementation with external pattern configuration
 */
public class ConfigurableFlinkCEP {
    
    private static final Logger LOG = LoggerFactory.getLogger(ConfigurableFlinkCEP.class);
    private static FraudPatternConfig patternConfig;
    
    public static void main(String[] args) throws Exception {
        String configPath = args.length > 0 ? args[0] : "fraud-patterns-config.json";
        
        // Initialize configuration
        patternConfig = new FraudPatternConfig(configPath);
        
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        LOG.info("Starting Configurable Flink CEP with configuration: {}", configPath);
        LOG.info("Configuration version: {}", patternConfig.getConfigVersion());
        
        // Create transaction stream (replace with your actual source)
        DataStream<Transaction> transactions = env.addSource(new TransactionSource());
        
        // Apply configurable fraud patterns
        DataStream<FraudAlert> fraudAlerts = applyConfigurablePatterns(transactions);
        
        // Process and sink alerts
        fraudAlerts.print("FRAUD ALERT");
        
        env.execute("Configurable Flink CEP Fraud Detection");
    }
    
    /**
     * Apply all enabled fraud patterns from configuration
     */
    private static DataStream<FraudAlert> applyConfigurablePatterns(DataStream<Transaction> transactions) {
        List<FraudPattern> enabledPatterns = patternConfig.getEnabledPatterns();
        LOG.info("Applying {} enabled fraud patterns", enabledPatterns.size());
        
        DataStream<FraudAlert> allAlerts = null;
        
        for (FraudPattern pattern : enabledPatterns) {
            LOG.info("Configuring pattern: {} ({})", pattern.getName(), pattern.getId());
            
            DataStream<FraudAlert> patternAlerts = null;
            
            switch (pattern.getId()) {
                case "HIGH_FREQUENCY_PATTERN":
                    patternAlerts = applyHighFrequencyPattern(transactions, pattern);
                    break;
                    
                case "SEQUENTIAL_LARGE_AMOUNTS":
                    patternAlerts = applySequentialPattern(transactions, pattern);
                    break;
                    
                case "UNUSUAL_AMOUNT_THRESHOLD":
                    patternAlerts = applyAmountThresholdPattern(transactions, pattern);
                    break;
                    
                case "VELOCITY_PATTERN":
                    patternAlerts = applyVelocityPattern(transactions, pattern);
                    break;
                    
                default:
                    LOG.warn("Unknown pattern type: {}", pattern.getId());
                    continue;
            }
            
            if (patternAlerts != null) {
                if (allAlerts == null) {
                    allAlerts = patternAlerts;
                } else {
                    allAlerts = allAlerts.union(patternAlerts);
                }
            }
        }
        
        return allAlerts != null ? allAlerts : transactions.map(t -> null).filter(alert -> false);
    }
    
    /**
     * Apply high frequency pattern using CEP
     */
    private static DataStream<FraudAlert> applyHighFrequencyPattern(
            DataStream<Transaction> transactions, FraudPattern pattern) {
        
        Map<String, Object> params = pattern.getParameters();
        int windowMinutes = (Integer) params.get("windowSizeMinutes");
        int minCount = (Integer) params.get("minimumTransactionCount");
        double minAmount = (Double) params.get("minimumTotalAmount");
        
        // Create CEP pattern
        Pattern<Transaction, ?> cepPattern = Pattern
                .<Transaction>begin("start")
                .where(new IterativeCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction transaction, Context<Transaction> context) {
                        return transaction.getAmount() > 0;
                    }
                })
                .timesOrMore(minCount)
                .within(Time.minutes(windowMinutes));
        
        // Apply pattern to keyed stream
        PatternStream<Transaction> patternStream = CEP.pattern(
                transactions.keyBy(Transaction::getAccountId), cepPattern);
        
        // Select matching patterns
        return patternStream.select(new PatternSelectFunction<Transaction, FraudAlert>() {
            @Override
            public FraudAlert select(Map<String, List<Transaction>> patternMatches) {
                List<Transaction> matches = patternMatches.get("start");
                double totalAmount = matches.stream().mapToDouble(Transaction::getAmount).sum();
                
                if (totalAmount >= minAmount) {
                    Transaction lastTx = matches.get(matches.size() - 1);
                    return new FraudAlert(
                            lastTx.getAccountId(),
                            pattern.getId(),
                            pattern.getName(),
                            String.format("High frequency: %d transactions, total: $%.2f", 
                                    matches.size(), totalAmount),
                            params.get("alertLevel").toString(),
                            System.currentTimeMillis()
                    );
                }
                return null;
            }
        }).filter(alert -> alert != null);
    }
    
    /**
     * Apply sequential large amounts pattern using CEP
     */
    private static DataStream<FraudAlert> applySequentialPattern(
            DataStream<Transaction> transactions, FraudPattern pattern) {
        
        Map<String, Object> params = pattern.getParameters();
        int sequenceLength = (Integer) params.get("minimumSequenceLength");
        int timeWindowHours = (Integer) params.get("timeWindowHours");
        double minTotalAmount = (Double) params.get("minimumTotalAmount");
        
        // Create sequential pattern
        Pattern<Transaction, ?> sequentialPattern = Pattern
                .<Transaction>begin("first")
                .where(new IterativeCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction transaction, Context<Transaction> context) {
                        return transaction.getAmount() > 100; // Base threshold
                    }
                })
                .followedBy("second")
                .where(new IterativeCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction transaction, Context<Transaction> context) {
                        return transaction.getAmount() > 200; // Increasing amounts
                    }
                })
                .followedBy("third")
                .where(new IterativeCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction transaction, Context<Transaction> context) {
                        return transaction.getAmount() > 300; // Further increase
                    }
                })
                .within(Time.hours(timeWindowHours));
        
        PatternStream<Transaction> patternStream = CEP.pattern(
                transactions.keyBy(Transaction::getAccountId), sequentialPattern);
        
        return patternStream.select(new PatternSelectFunction<Transaction, FraudAlert>() {
            @Override
            public FraudAlert select(Map<String, List<Transaction>> patternMatches) {
                List<Transaction> first = patternMatches.get("first");
                List<Transaction> second = patternMatches.get("second");
                List<Transaction> third = patternMatches.get("third");
                
                double totalAmount = 0;
                totalAmount += first.stream().mapToDouble(Transaction::getAmount).sum();
                totalAmount += second.stream().mapToDouble(Transaction::getAmount).sum();
                totalAmount += third.stream().mapToDouble(Transaction::getAmount).sum();
                
                if (totalAmount >= minTotalAmount) {
                    Transaction lastTx = third.get(third.size() - 1);
                    return new FraudAlert(
                            lastTx.getAccountId(),
                            pattern.getId(),
                            pattern.getName(),
                            String.format("Sequential escalation: $%.2f total", totalAmount),
                            params.get("alertLevel").toString(),
                            System.currentTimeMillis()
                    );
                }
                return null;
            }
        }).filter(alert -> alert != null);
    }
    
    /**
     * Apply amount threshold pattern using simple filter
     */
    private static DataStream<FraudAlert> applyAmountThresholdPattern(
            DataStream<Transaction> transactions, FraudPattern pattern) {
        
        Map<String, Object> params = pattern.getParameters();
        double threshold = (Double) params.get("largeAmountThreshold");
        
        return transactions
                .filter(new FilterFunction<Transaction>() {
                    @Override
                    public boolean filter(Transaction transaction) {
                        return transaction.getAmount() > threshold;
                    }
                })
                .map(new MapFunction<Transaction, FraudAlert>() {
                    @Override
                    public FraudAlert map(Transaction transaction) {
                        return new FraudAlert(
                                transaction.getAccountId(),
                                pattern.getId(),
                                pattern.getName(),
                                String.format("Large amount: $%.2f", transaction.getAmount()),
                                params.get("alertLevel").toString(),
                                System.currentTimeMillis()
                        );
                    }
                });
    }
    
    /**
     * Apply velocity pattern using stateful processing
     */
    private static DataStream<FraudAlert> applyVelocityPattern(
            DataStream<Transaction> transactions, FraudPattern pattern) {
        
        Map<String, Object> params = pattern.getParameters();
        int maxTimeMinutes = (Integer) params.get("maxTimeBetweenTransactionsMinutes");
        
        return transactions
                .keyBy(Transaction::getAccountId)
                .process(new KeyedProcessFunction<String, Transaction, FraudAlert>() {
                    
                    private ValueState<Long> lastTransactionTime;
                    
                    @Override
                    public void open(Configuration parameters) {
                        lastTransactionTime = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("lastTxTime", Long.class));
                    }
                    
                    @Override
                    public void processElement(Transaction transaction, Context context, 
                                               Collector<FraudAlert> collector) throws Exception {
                        
                        Long lastTime = lastTransactionTime.value();
                        long currentTime = transaction.getTimestamp();
                        
                        if (lastTime != null) {
                            long timeDiffMinutes = (currentTime - lastTime) / (1000 * 60);
                            
                            if (timeDiffMinutes <= maxTimeMinutes) {
                                collector.collect(new FraudAlert(
                                        transaction.getAccountId(),
                                        pattern.getId(),
                                        pattern.getName(),
                                        String.format("High velocity: %d minutes between transactions", 
                                                timeDiffMinutes),
                                        params.get("alertLevel").toString(),
                                        System.currentTimeMillis()
                                ));
                            }
                        }
                        
                        lastTransactionTime.update(currentTime);
                    }
                });
    }
    
    // Data classes
    public static class Transaction {
        private String accountId;
        private double amount;
        private long timestamp;
        private String merchantCategory;
        
        public Transaction() {}
        
        public Transaction(String accountId, double amount, long timestamp, String merchantCategory) {
            this.accountId = accountId;
            this.amount = amount;
            this.timestamp = timestamp;
            this.merchantCategory = merchantCategory;
        }
        
        // Getters and setters
        public String getAccountId() { return accountId; }
        public void setAccountId(String accountId) { this.accountId = accountId; }
        
        public double getAmount() { return amount; }
        public void setAmount(double amount) { this.amount = amount; }
        
        public long getTimestamp() { return timestamp; }
        public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
        
        public String getMerchantCategory() { return merchantCategory; }
        public void setMerchantCategory(String merchantCategory) { this.merchantCategory = merchantCategory; }
    }
    
    public static class FraudAlert {
        private String accountId;
        private String patternId;
        private String patternName;
        private String message;
        private String alertLevel;
        private long timestamp;
        
        public FraudAlert() {}
        
        public FraudAlert(String accountId, String patternId, String patternName, 
                         String message, String alertLevel, long timestamp) {
            this.accountId = accountId;
            this.patternId = patternId;
            this.patternName = patternName;
            this.message = message;
            this.alertLevel = alertLevel;
            this.timestamp = timestamp;
        }
        
        // Getters and setters
        public String getAccountId() { return accountId; }
        public void setAccountId(String accountId) { this.accountId = accountId; }
        
        public String getPatternId() { return patternId; }
        public void setPatternId(String patternId) { this.patternId = patternId; }
        
        public String getPatternName() { return patternName; }
        public void setPatternName(String patternName) { this.patternName = patternName; }
        
        public String getMessage() { return message; }
        public void setMessage(String message) { this.message = message; }
        
        public String getAlertLevel() { return alertLevel; }
        public void setAlertLevel(String alertLevel) { this.alertLevel = alertLevel; }
        
        public long getTimestamp() { return timestamp; }
        public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
        
        @Override
        public String toString() {
            return String.format("FraudAlert{accountId='%s', pattern='%s', level='%s', message='%s'}", 
                    accountId, patternName, alertLevel, message);
        }
    }
    
    // Sample transaction source
    public static class TransactionSource implements SourceFunction<Transaction> {
        private volatile boolean running = true;
        
        @Override
        public void run(SourceContext<Transaction> ctx) throws Exception {
            String[] accounts = {"ACC001", "ACC002", "ACC003", "ACC004", "ACC005"};
            String[] merchants = {"GROCERY", "GAS", "RESTAURANT", "ONLINE", "ATM"};
            
            int counter = 0;
            while (running && counter < 1000) {
                String accountId = accounts[counter % accounts.length];
                double amount = 50 + (Math.random() * 5000); // Random amount between 50-5050
                String merchant = merchants[counter % merchants.length];
                
                Transaction tx = new Transaction(accountId, amount, System.currentTimeMillis(), merchant);
                ctx.collect(tx);
                
                Thread.sleep(100); // 100ms between transactions
                counter++;
            }
        }
        
        @Override
        public void cancel() {
            running = false;
        }
    }
}
