//package com.zhugeio.etl.pipeline.operator.id;
//
//import com.alibaba.fastjson2.JSONArray;
//import com.alibaba.fastjson2.JSONObject;
//import com.github.benmanes.caffeine.cache.Cache;
//import com.github.benmanes.caffeine.cache.Caffeine;
//import com.zhugeio.etl.common.client.kvrocks.KvrocksClient;
//import com.zhugeio.etl.pipeline.example.ZGMessage;
//import com.zhugeio.etl.pipeline.util.SnowflakeIdGenerator;
//import org.apache.commons.lang3.StringUtils;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.functions.async.ResultFuture;
//import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
//
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.List;
//import java.util.concurrent.CompletableFuture;
//import java.util.concurrent.TimeUnit;
//
///**
// * 用户ID映射算子 (雪花算法版本)
// */
//public class UserIdAsyncOperator extends RichAsyncFunction<ZGMessage, ZGMessage> {
//
//    private transient KvrocksClient kvrocks;
//    private transient Cache<String, Long> localCache;
//    private transient SnowflakeIdGenerator idGenerator;  // ✅ 新增
//
//    private final String kvrocksHost;
//    private final int kvrocksPort;
//    private final boolean kvrocksCluster;
//
//    public UserIdAsyncOperator() {
//        this(null, 0, true);
//    }
//
//    public UserIdAsyncOperator(String kvrocksHost, int kvrocksPort) {
//        this(kvrocksHost, kvrocksPort, true);
//    }
//
//    public UserIdAsyncOperator(String kvrocksHost, int kvrocksPort, boolean kvrocksCluster) {
//        this.kvrocksHost = kvrocksHost;
//        this.kvrocksPort = kvrocksPort;
//        this.kvrocksCluster = kvrocksCluster;
//    }
//
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        kvrocks = new KvrocksClient(kvrocksHost, kvrocksPort, kvrocksCluster);
//        kvrocks.init();
//
//        int workerId = generateSnowflakeWorkerId();
//        int totalSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
//
//        if (totalSubtasks > 1024) {
//            throw new RuntimeException(
//                    "UserId算子最多支持1024个并行度,当前: " + totalSubtasks);
//        }
//
//        idGenerator = new SnowflakeIdGenerator(workerId);
//
//        localCache = Caffeine.newBuilder()
//                .maximumSize(10000)
//                .expireAfterWrite(10, TimeUnit.MINUTES)
//                .build();
//
//        System.out.printf(
//                "[UserId算子-%d] 雪花算法初始化成功, workerId=%d%n",
//                getRuntimeContext().getIndexOfThisSubtask(), workerId
//        );
//    }
//
//    @Override
//    public void asyncInvoke(ZGMessage input, ResultFuture<ZGMessage> resultFuture) {
//        JSONArray data = (JSONArray) input.getData();
//        Integer appId = input.getAppId();
//
//        if (data == null || data.isEmpty()) {
//            resultFuture.complete(Collections.singleton(input));
//            return;
//        }
//
//        // 收集所有异步操作
//        List<CompletableFuture<Object>> futures = new ArrayList<>();
//
//        for (int i = 0; i < data.size(); i++) {
//            JSONObject item = data.getJSONObject(i);
//            if (item == null) continue;
//
//            Object pr = item.get("pr");
//            if (!(pr instanceof JSONObject)) continue;
//
//            JSONObject prObject = (JSONObject) pr;
//
//            // 提取并清理 cuid
//            if (!prObject.containsKey("$cuid") || prObject.get("$cuid") == null) {
//                prObject.remove("$cuid");
//                continue;
//            }
//
//            String cuid = String.valueOf(prObject.get("$cuid")).trim();
//            if (StringUtils.isBlank(cuid)) {
//                prObject.remove("$cuid");
//                continue;
//            }
//
//            prObject.put("$cuid", cuid);
//            String cacheKey = "u:" + appId + ":" + cuid;
//
//            // 先查本地缓存
//            Long cachedZgUid = localCache.getIfPresent(cacheKey);
//            if (cachedZgUid != null) {
//                prObject.put("$zg_uid", cachedZgUid);
//                continue;
//            }
//
//            // 异步查询 KV
//            String hashKey = "u:" + appId;
//            CompletableFuture<Object> future = kvrocks.asyncHGet(hashKey, cuid)
//                    .thenCompose(zgUidStr -> {
//                        Long zgUserId;
//                        if (zgUidStr != null) {
//                            // KV 中存在，直接使用
//                            zgUserId = Long.parseLong(zgUidStr);
//                        } else {
//                            // KV 中不存在，生成新 ID 并异步写入
//                            zgUserId = idGenerator.nextId();
//                            Long finalZgUserId = zgUserId;
//                            kvrocks.asyncHSet(hashKey, cuid, String.valueOf(zgUserId))
//                                    .exceptionally(ex -> {
//                                        System.err.println("UserId写入失败: " + hashKey + ", cuid: " + cuid);
//                                        ex.printStackTrace();
//                                        return null;
//                                    });
//                        }
//                        localCache.put(cacheKey, zgUserId);
//                        prObject.put("$zg_uid", zgUserId);
//                        return CompletableFuture.completedFuture(null);
//                    })
//                    .exceptionally(ex -> {
//                        System.err.println("UserId查询失败: " + cacheKey);
//                        ex.printStackTrace();
//                        return null;
//                    });
//
//            futures.add(future);
//        }
//
//        // 等待所有异步操作完成后，返回修改后的 input
//        if (futures.isEmpty()) {
//            resultFuture.complete(Collections.singleton(input));
//        } else {
//            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
//                    .whenComplete((v, throwable) -> {
//                        if (throwable != null) {
//                            System.err.println("批量处理UserId时出错");
//                            throwable.printStackTrace();
//                        }
//                        resultFuture.complete(Collections.singleton(input));
//                    });
//        }
//    }
//
//    @Override
//    public void close() {
//        if (kvrocks != null) {
//            kvrocks.shutdown();
//        }
//
//        if (localCache != null) {
//            System.out.printf(
//                    "[UserId算子-%d] 缓存统计: %s%n",
//                    getRuntimeContext().getIndexOfThisSubtask(),
//                    localCache.stats()
//            );
//        }
//    }
//
//    /**
//     * 使用主机名和slot ID组合生成workerID，确保在分布式环境中的唯一性
//     * @return workerId
//     */
//    private int generateSnowflakeWorkerId() {
//        try {
//            // 使用主机信息和 slot 组合来生成 workerId
//            String hostName = java.net.InetAddress.getLocalHost().getHostName();
//            int slotId = getRuntimeContext().getIndexOfThisSubtask();
//
//            return Math.abs((hostName.hashCode() * 31 + slotId)) % 256;
//        } catch (Exception e) {
//            // 回退到 subtask index
//            return getRuntimeContext().getIndexOfThisSubtask() % 256;
//        }
//    }
//}