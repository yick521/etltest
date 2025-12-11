//package com.zhugeio.etl.pipeline.operator.id;
//
//import com.github.benmanes.caffeine.cache.Cache;
//import com.github.benmanes.caffeine.cache.Caffeine;
//import com.zhugeio.etl.common.client.kvrocks.KvrocksClient;
//import com.zhugeio.etl.pipeline.enums.ErrorMessageEnum;
//import com.zhugeio.etl.pipeline.example.ZGMessage;
//import com.zhugeio.etl.pipeline.model.CompanyApp;
//import com.zhugeio.etl.pipeline.util.Dims;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.functions.async.ResultFuture;
//import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
//
//import java.util.Collections;
//import java.util.concurrent.CompletableFuture;
//import java.util.concurrent.TimeUnit;
//
///**
// * 设置AppId和Business异步算子
// * <p>
// * 优化点:
// * 1. ✅ 使用缓存减少重复查询
// * 2. ✅ 所有操作都是异步的
// * 3. ✅ 支持Kvrocks集群和单机模式
// */
//public class SetAppIdAndBusinessOperator extends RichAsyncFunction<ZGMessage, ZGMessage> {
//
//    private transient KvrocksClient kvrocks;
//    private transient Cache<String, CompanyApp> appCache;
//
//    private final String kvrocksHost;
//    private final int kvrocksPort;
//    private final boolean kvrocksCluster;
//
//    public SetAppIdAndBusinessOperator(String kvrocksHost, int kvrocksPort, boolean kvrocksCluster) {
//        this.kvrocksHost = kvrocksHost;
//        this.kvrocksPort = kvrocksPort;
//        this.kvrocksCluster = kvrocksCluster;
//    }
//
//    @Override
//    public void open(Configuration parameters) {
//        // 初始化KVRocks客户端
//        kvrocks = new KvrocksClient(kvrocksHost, kvrocksPort, kvrocksCluster);
//        kvrocks.init();
//
//        // 测试连接
//        if (!kvrocks.testConnection()) {
//            throw new RuntimeException("KVRocks连接失败!");
//        }
//
//        // 初始化Caffeine缓存
//        appCache = Caffeine.newBuilder()
//                .maximumSize(100000)
//                .expireAfterWrite(10, TimeUnit.MINUTES)
//                .recordStats()
//                .build();
//
//    }
//
//    @Override
//    public void asyncInvoke(ZGMessage input, ResultFuture<ZGMessage> resultFuture) {
//
//        String ak = input.getAppKey();
//        Integer plat = Dims.sdk(String.valueOf(input.getData().get("pl")));
//
//        CompanyApp companyApp = appCache.getIfPresent(ak);
//
//        if (companyApp != null) {
//            ZGMessage output = createOutput(input, companyApp);
//            resultFuture.complete(Collections.singleton(output));
//            return;
//        }
//
//        String hashKey = "company_app";
//
//        kvrocks.asyncHGetAll(hashKey)
//                .thenCompose(map -> {
//                    if (map != null && map.containsKey(ak)) {
//                        String appStr = map.get(ak);
//                        CompanyApp app = CompanyApp.fromJson(appStr);
//                        appCache.put(ak, app);
//                        ZGMessage output = createOutput(input, app);
//
//                        // todo: 回写MySQL表
//                        // FrontService.handleAppPlat(appIdOption.get, plat)
//                        // FrontService.handleAppUpload(appIdOption.get, plat)
//                        return CompletableFuture.completedFuture(output);
//                    } else {
//                        // 如果未找到对应appKey的数据，则标记为错误
//                        ZGMessage output = createErrorOutput(input);
//                        return CompletableFuture.completedFuture(output);
//                    }
//                })
//                .whenComplete((output, throwable) -> {
//                    if (throwable != null) {
//                        resultFuture.completeExceptionally(throwable);
//                    } else {
//                        resultFuture.complete(Collections.singleton(output));
//                    }
//                });
//    }
//
//    private ZGMessage createOutput(ZGMessage input, CompanyApp companyApp) {
//
//        String business = input.getData().getOrDefault("business", "").toString();
//
//        ZGMessage output = new ZGMessage();
//        output.setTopic(input.getTopic());
//        output.setPartition(input.getPartition());
//        output.setOffset(input.getOffset());
//        output.setKey(input.getKey());
//        output.setRawData(input.getRawData());
//        output.setResult(input.getResult());
//        output.setAppKey(input.getAppKey());
//        output.setSdk(input.getSdk());
//        output.setData(input.getData());
//        output.setErrData(input.getErrData());
//        output.setJson(input.getJson());
//        output.setError(input.getError());
//        output.setErrorCode(input.getErrorCode());
//        output.setErrorDescribe(input.getErrorDescribe());
//        output.setBusiness(business);
//
//        if (companyApp != null) {
//            output.setAppId(companyApp.getId());
//        } else {
//            output.setAppId(0);
//        }
//
//        return output;
//    }
//
//    private ZGMessage createErrorOutput(ZGMessage input) {
//        ZGMessage output = new ZGMessage();
//
//        output.setTopic(input.getTopic());
//        output.setPartition(input.getPartition());
//        output.setOffset(input.getOffset());
//        output.setKey(input.getKey());
//        output.setRawData(input.getRawData());
//        output.setResult(-1);
//        output.setAppKey(input.getAppKey());
//        output.setSdk(input.getSdk());
//        output.setData(input.getData());
//        output.setErrData(input.getData());
//        int errorCode = ErrorMessageEnum.AK_NONE.getErrorCode();
//        String errorInfo = ErrorMessageEnum.AK_NONE.getErrorMessage();
//
//        output.setErrorCode(errorCode);
//        output.setErrorDescribe(errorInfo);
//        output.setError(errorInfo);
//        output.setAppId(0);
//        output.setBusiness("");
//
//        return output;
//    }
//
//    @Override
//    public void close() {
//        if (kvrocks != null) {
//            kvrocks.shutdown();
//        }
//
//        if (appCache != null) {
//            System.out.printf(
//                    "[SetAppIdAndBusinessOperator-%d] 缓存统计: %s%n",
//                    getRuntimeContext().getIndexOfThisSubtask(),
//                    appCache.stats()
//            );
//        }
//    }
//}