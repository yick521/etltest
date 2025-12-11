package com.zhugeio.etl.common.sink;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;;

/**
 * 通用 Doris Sink 工厂
 *
 * 特性:
 * 1. 支持灵活配置
 * 2. 支持部分列更新
 * 3. 支持不同序列化策略
 * 4. 线程安全
 */
public class DorisSinkFactory {

    /**
     * 创建 Doris Sink
     *
     * @param config Sink配置
     * @param <T> 数据类型
     * @return DorisSink (Flink 1.17 返回 DorisSink,它实现了 SinkFunction)
     */
    public static <T> DorisSink<T> create(DorisSinkConfig config) {
        // 构建 DorisOptions
        DorisOptions dorisOptions = DorisOptions.builder()
                .setFenodes(config.getFeNodes())
                .setTableIdentifier(config.getDatabase() + "." + config.getTable())
                .setUsername(config.getUsername())
                .setPassword(config.getPassword())
                .build();

        // 构建 DorisExecutionOptions
        DorisExecutionOptions executionOptions = DorisExecutionOptions.builder()
                .setBufferSize(config.getBatchSize())
                .setBufferFlushMaxRows(config.getBatchSize())
                .setBufferFlushIntervalMs(config.getBatchIntervalMs())
                .setMaxRetries(config.getMaxRetries())
                .setStreamLoadProp(config.toStreamLoadProperties())
                .setLabelPrefix(config.getLabelPrefix())
                .build();
        // 构建 DorisReadOptions
        DorisReadOptions readOptions = DorisReadOptions.builder().build();

        // 选择序列化器
        DorisRecordSerializer<T> serializer = createSerializer(config);

        // 构建 Sink
        return DorisSink.<T>builder()
                .setDorisReadOptions(readOptions)
                .setDorisOptions(dorisOptions)
                .setDorisExecutionOptions(executionOptions)
                .setSerializer(serializer)
                .build();
    }

    /**
     * 创建序列化器
     */
    private static <T> DorisRecordSerializer<T> createSerializer(DorisSinkConfig config) {
        String serializerType = config.getSerializerType();
        String includeStrategy = config.getIncludeStrategy();

        if ("jackson".equalsIgnoreCase(serializerType)) {
            return JsonSerializerFactory.createJacksonSerializer(includeStrategy);
        } else {
            // 默认使用Jackson
            return JsonSerializerFactory.createDefaultSerializer();
        }
    }

    /**
     * 创建快捷配置 - 部分列更新场景 (用户表、设备表)
     */
    public static DorisSinkConfig partialUpdateConfig(
            String feNodes,
            String username,
            String password,
            String database,
            String table) {

        return DorisSinkConfig.builder()
                .feNodes(feNodes)
                .username(username)
                .password(password)
                .database(database)
                .table(table)
                .batchSize(5000)
                .batchIntervalMs(30000)
                .maxRetries(5)
                .partialUpdate(true)        // 启用部分列更新
                .mergeType("MERGE")
                .strictMode(true)           // 严格模式
                .maxFilterRatio("0")        // 不允许错误
                .labelPrefix("partial-update")
                .serializerType("jackson")
                .includeStrategy("NON_NULL")
                .build();
    }

    /**
     * 创建快捷配置 - 高吞吐场景 (事件表)
     */
    public static DorisSinkConfig highThroughputConfig(
            String feNodes,
            String username,
            String password,
            String database,
            String table) {

        return DorisSinkConfig.builder()
                .feNodes(feNodes)
                .username(username)
                .password(password)
                .database(database)
                .table(table)
                .batchSize(50000)           // 大批次
                .batchIntervalMs(5000)      // 短间隔
                .maxRetries(2)
                .partialUpdate(false)
                .strictMode(false)
                .maxFilterRatio("0.05")     // 允许5%错误
                .loadMemLimit("8589934592") // 8GB
                .labelPrefix("high-throughput")
                .serializerType("jackson")
                .includeStrategy("NON_NULL")
                .build();
    }

    /**
     * 创建快捷配置 - 低频更新场景
     */
    public static DorisSinkConfig lowFrequencyConfig(
            String feNodes,
            String username,
            String password,
            String database,
            String table) {

        return DorisSinkConfig.builder()
                .feNodes(feNodes)
                .username(username)
                .password(password)
                .database(database)
                .table(table)
                .batchSize(1000)            // 小批次
                .batchIntervalMs(60000)     // 1分钟
                .maxRetries(5)
                .partialUpdate(false)
                .strictMode(true)
                .maxFilterRatio("0")
                .labelPrefix("low-frequency")
                .serializerType("jackson")
                .includeStrategy("NON_NULL")
                .build();
    }
}