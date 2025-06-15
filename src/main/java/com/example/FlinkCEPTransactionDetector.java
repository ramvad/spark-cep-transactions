package com.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.apache.flink.configuration.Configuration;


import java.time.Duration;
import java.util.List;

public class FlinkCEPTransactionDetector {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkCEPTransactionDetector.class);

    public static class Transaction {
        public String accountId;
        public double amount;
        public long timestamp;

        public Transaction() {}

        public Transaction(String accountId, double amount, long timestamp) {
            this.accountId = accountId;
            this.amount = amount;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return accountId + "," + amount + "," + timestamp;
        }
    }

    public static void main(String[] args) throws Exception {
        
 System.out.println(">>> FlinkCEPTransactionDetector start");
        LOG.info(">>> FlinkCEPTransactionDetector starting...");
        
        Configuration config = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
 

        // Create FileSource using TextLineFormat (Flink 1.17.1)
        FileSource<String> source = FileSource.forRecordStreamFormat(
                new TextLineFormat(),
                new Path("C:/Users/svgks/Downloads/flink-cep-transactions/transactions.csv")
        ).build();

        // Create data stream from source, process once
        DataStream<String> input = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "file-source"
        );

        input.print("RAW INPUT");

        DataStream<Transaction> transactions = input
                .filter(line -> !line.startsWith("timestamp"))
                .map((MapFunction<String, Transaction>) line -> {
                    String[] fields = line.split(",");
                    long ts = java.sql.Timestamp.valueOf(fields[0]).getTime();
                    return new Transaction(fields[1], Double.parseDouble(fields[2]), ts);
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, ts) -> event.timestamp)
                );

        Pattern<Transaction, ?> pattern = Pattern.<Transaction>begin("first")
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction t) {
                        return t.amount > 0;
                    }
                })
                .next("second")
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction t) {
                        return t.amount > 0;
                    }
                })
                .next("third")
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction t) {
                        return t.amount > 0;
                    }
                })
                .within(Time.hours(1));

        PatternStream<Transaction> patternStream = CEP.pattern(transactions.keyBy(t -> t.accountId), pattern);

        SingleOutputStreamOperator<String> alerts = patternStream.select(
                (PatternSelectFunction<Transaction, String>) patternMatch -> {
                    List<Transaction> events = List.of(
                            patternMatch.get("first").get(0),
                            patternMatch.get("second").get(0),
                            patternMatch.get("third").get(0)
                    );
                    double total = events.stream().mapToDouble(e -> e.amount).sum();
                    if (total > 1000.0) {
                        return "ALERT: " + events.get(0).accountId + " total $" + total;
                    }
                    return null;
                }
        ).filter(msg -> msg != null);

        alerts.print("ALERTS");

        env.execute("Flink CEP Transaction Pattern");
        System.out.println(">>> FlinkCEPTransactionDetector ending");
    }
}
