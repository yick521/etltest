package com.zhugeio.etl.pipeline.example;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import java.util.Properties;

/**
 * @author ningjh
 * @name App
 * @date 2025/11/28
 * @description
 */
public class ExampleApplication {
    public static void main(String[] args) {
        System.out.println("start");
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.enableCheckpointing(30 * 1000L);
            env.getCheckpointConfig().setCheckpointInterval(30 * 1000L);
            env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
            env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
            env.getCheckpointConfig().setTolerableCheckpointFailureNumber(5);
//            env.setStateBackend(new RocksDBStateBackend("hdfs:///user/flink/checkpoints/", true));
            // 1. Kafka配置
            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", "realtime-1:9092,realtime-3:9092,realtime-3:9092");
            properties.setProperty("group.id", "test-em");
            properties.setProperty("auto.offset.reset", "earliest");
            properties.setProperty("enable.auto.commit", "false"); // 必须禁用自动提交
            properties.setProperty("flink.partition-discovery.interval-millis", "30000"); // 30秒发现新分区

            // 2. 创建Kafka Source
            FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>(
                    "sdklua_online",
                    new SimpleStringSchema(),
                    properties
            );

            flinkKafkaConsumer.setCommitOffsetsOnCheckpoints(true); // 偏移量提交到检查点

            DataStreamSource<String> source = env.addSource(flinkKafkaConsumer);

            source.print();

            env.execute("test");

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
