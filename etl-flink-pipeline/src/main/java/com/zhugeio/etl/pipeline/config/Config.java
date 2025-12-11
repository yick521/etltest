package com.zhugeio.etl.pipeline.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

/**
 * 配置管理类
 * 
 * 配置优先级: 系统属性 > 环境变量 > 配置文件
 */
public class Config {
    
    private static final Logger LOG = LoggerFactory.getLogger(Config.class);
    
    // 配置文件
    private static final String CONFIG_FILE = "config.properties";
    
    // 配置缓存
    private static final Properties properties = new Properties();
    
    // ============ Kafka 配置 ============
    public static final String KAFKA_BROKERS = "kafka.brokers";
    public static final String KAFKA_SOURCE_TOPIC = "kafka.sourceTopic";
    public static final String KAFKA_GROUP_ID = "kafka.group.id";
    public static final String KAFKA_SASL_USERNAME = "kafka.sasl.username";
    public static final String KAFKA_SASL_PASSWORD = "kafka.sasl.password";
    public static final String KAFKA_BUFFER_MEMORY = "kafka.buffer.memory";
    public static final String KAFKA_SEND_OPEN = "kafka.send.open";
    public static final String KAFKA_SEND_TOPIC = "kafka.send.topic";
    public static final String KAFKA_SEND_BROKERS = "kafka.send.brokers";
    
    // ============ Flink 配置 ============
    public static final String FLINK_PARALLELISM = "flink.parallelism";
    public static final String FLINK_CHECKPOINT_INTERVAL = "flink.checkpoint.interval";
    public static final String FLINK_CHECKPOINT_TIMEOUT = "flink.checkpoint.timeout";
    
    // ============ KVRocks 配置 ============
    public static final String KVROCKS_HOST = "kvrocks.host";
    public static final String KVROCKS_PORT = "kvrocks.port";
    public static final String KVROCKS_PASSWORD = "kvrocks.password";
    public static final String KVROCKS_CLUSTER = "kvrocks.cluster";
    public static final String KVROCKS_LOCAL_CACHE_SIZE = "kvrocks.local.cache.size";
    public static final String KVROCKS_LOCAL_CACHE_EXPIRE_MINUTES = "kvrocks.local.cache.expire.minutes";
    
    // ============ Redis 配置 (百度关键词缓存) ============
    public static final String REDIS_HOST = "redis.host";
    public static final String REDIS_PORT = "redis.port";
    public static final String REDIS_PASSWORD = "redis.password";
    public static final String REDIS_CLUSTER = "redis.cluster";
    
    // ============ Doris 配置 ============
    public static final String DORIS_HOST = "doris.host";
    public static final String DORIS_HTTP_PORT = "doris.http.port";
    public static final String DORIS_DB = "doris.db";
    public static final String DORIS_USER = "doris.user";
    public static final String DORIS_PASSWORD = "doris.password";
    
    // ============ IP 解析配置 ============
    public static final String IP_FILE_DIR = "ip.file.dir";
    public static final String IPV6_FILE_DIR = "ipv6.file.dir";
    public static final String IPV6_LOAD = "ipv6.load";
    public static final String RELOAD_IP_FILE = "reload.ip.file";
    public static final String RELOAD_IPV6_FILE = "reload.ipv6.file";
    public static final String RELOAD_RATE_SECOND = "reload.rate.second";
    public static final String FLAG_HA = "flag.ha";
    
    // ============ HDFS 配置 ============
    public static final String FS_DEFAULT_FS = "fs.defaultFS";
    public static final String DFS_NAMESERVICES = "dfs.nameservices";
    public static final String DFS_HA_NAMESPACE = "dfs.ha.namenodes.namespace";
    public static final String RPC_Z1 = "dfs.namenode.rpc-address.namespace.z1";
    public static final String RPC_Z2 = "dfs.namenode.rpc-address.namespace.z2";
    
    // ============ 百度 API 配置 ============
    public static final String BAIDU_URL = "baidu.url";
    public static final String BAIDU_ID = "baidu_id";
    public static final String BAIDU_KEY = "baidu_key";
    
    // ============ HTTP 配置 ============
    public static final String REQUEST_SOCKET_TIMEOUT = "requestSocketTimeout";
    public static final String REQUEST_CONNECT_TIMEOUT = "requestConnectTimeout";
    public static final String REQUEST_TIMEOUT = "requestTimeout";
    public static final String MAX_RETRY_NUM = "maxRetryNum";
    public static final String BATCH_SIZE = "batchSize";
    
    // ============ 时间配置 ============
    public static final String TIME_EXPIRE_SUBDAYS = "subtime";
    public static final String TIME_EXPIRE_ADDDAYS = "addtime";
    
    // ============ 应用配置 ============
    public static final String BLACK_APPIDS = "blackAppIds";
    public static final String WHITE_APPID = "white_appid";
    public static final String EVENT_ATTR_LENGTH_LIMIT = "event_attr_length_limit";
    public static final String WRITE_EVENT_ALL_FLAG = "write.event.all.flag";
    public static final String WRITE_EVENT_ATTR_EID_PARTITION = "write.event.attr.eid.partition";
    
    // ============ 数据库类型 ============
    public static final String DB_TYPE = "dbtype";  // 1=Kudu, 2=Doris
    
    // ============ 写入配置 ============
    public static final String WRITE_POOL = "write.pool";
    public static final String BATCH_DATA_NUM = "batch.dataNum";
    public static final String MUTATION_BUFFER = "mutation.buffer";
    
    // ============ 加密配置 ============
    public static final String ENCRYPTION_TYPE = "encryption_type";
    public static final String SM4_PRIKEY_PATH = "sm4_priKey_path";
    
    // 静态初始化
    static {
        loadProperties();
    }
    
    /**
     * 加载配置文件
     */
    private static void loadProperties() {
        try (InputStream is = Config.class.getClassLoader().getResourceAsStream(CONFIG_FILE)) {
            if (is != null) {
                properties.load(is);
                LOG.info("Loaded configuration from {}", CONFIG_FILE);
            } else {
                LOG.warn("Configuration file {} not found, using defaults", CONFIG_FILE);
            }
        } catch (Exception e) {
            LOG.error("Failed to load configuration file", e);
        }
    }
    
    /**
     * 获取字符串配置
     * 优先级: 系统属性 > 环境变量 > 配置文件 > 默认值
     */
    public static String getString(String key, String defaultValue) {
        // 1. 系统属性
        String value = System.getProperty(key);
        if (value != null && !value.isEmpty()) {
            return value;
        }
        
        // 2. 环境变量 (将 . 替换为 _)
        String envKey = key.replace('.', '_').toUpperCase();
        value = System.getenv(envKey);
        if (value != null && !value.isEmpty()) {
            return value;
        }
        
        // 3. 配置文件
        value = properties.getProperty(key);
        if (value != null && !value.isEmpty()) {
            return value;
        }
        
        // 4. 默认值
        return defaultValue;
    }
    
    /**
     * 获取字符串配置 (无默认值)
     */
    public static String getString(String key) {
        return getString(key, null);
    }
    
    /**
     * 获取整数配置
     */
    public static int getInt(String key, int defaultValue) {
        String value = getString(key, null);
        if (value != null) {
            try {
                return Integer.parseInt(value.trim());
            } catch (NumberFormatException e) {
                LOG.warn("Invalid integer value for {}: {}", key, value);
            }
        }
        return defaultValue;
    }
    
    /**
     * 获取整数配置 (无默认值)
     */
    public static int getInt(String key) {
        return getInt(key, 0);
    }
    
    /**
     * 获取长整数配置
     */
    public static long getLong(String key, long defaultValue) {
        String value = getString(key, null);
        if (value != null) {
            try {
                return Long.parseLong(value.trim());
            } catch (NumberFormatException e) {
                LOG.warn("Invalid long value for {}: {}", key, value);
            }
        }
        return defaultValue;
    }
    
    /**
     * 获取布尔配置
     */
    public static boolean getBoolean(String key, boolean defaultValue) {
        String value = getString(key, null);
        if (value != null) {
            return "true".equalsIgnoreCase(value.trim()) || "1".equals(value.trim());
        }
        return defaultValue;
    }
    
    /**
     * 获取字符串列表
     */
    public static String[] getStringArray(String key, String delimiter) {
        String value = getString(key, "");
        if (value.isEmpty()) {
            return new String[0];
        }
        return value.split(delimiter);
    }
    
    /**
     * 是否是 Doris 模式
     */
    public static boolean isDorisMode() {
        return getInt(DB_TYPE, 1) == 2;
    }
    
    /**
     * 是否是 Kudu 模式
     */
    public static boolean isKuduMode() {
        return getInt(DB_TYPE, 1) == 1;
    }
}
