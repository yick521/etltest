package com.zhugeio.etl.pipeline.operator.dw;

import com.zhugeio.etl.common.model.RawEvent;
import com.zhugeio.etl.common.util.ip.IpDatabaseLoader;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

/**
 * IP地理位置解析异步算子
 *
 * 优势:
 * 1. 纯内存查询,无网络IO
 * 2. 异步处理提高吞吐量
 * 3. 支持热加载IP数据库
 */
public class IpLocationAsyncOperator extends RichAsyncFunction<RawEvent, RawEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(IpLocationAsyncOperator.class);

    private transient IpDatabaseLoader ipLoader;

    private final String hdfsPath;
    private final boolean enableAutoReload;
    private final long reloadIntervalSeconds;
    private final boolean isHdfsHA;

    public IpLocationAsyncOperator(String hdfsPath,
                                   boolean enableAutoReload,
                                   long reloadIntervalSeconds,
                                   boolean isHdfsHA) {
        this.hdfsPath = hdfsPath;
        this.enableAutoReload = enableAutoReload;
        this.reloadIntervalSeconds = reloadIntervalSeconds;
        this.isHdfsHA = isHdfsHA;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();

        // 初始化IP数据库加载器
        ipLoader = new IpDatabaseLoader(
                hdfsPath,
                enableAutoReload,
                reloadIntervalSeconds,
                isHdfsHA
        );
        ipLoader.init();

        LOG.info("[IP解析算子-{}] 初始化成功", subtaskIndex);
    }

    @Override
    public void asyncInvoke(RawEvent input, ResultFuture<RawEvent> resultFuture) throws Exception {
        // 异步执行IP查询 (实际上是同步的,但放在异步上下文中以提高并发)
        CompletableFuture.supplyAsync(() -> {
            String ip = input.getIp();

            if (ip == null || ip.isEmpty() || "0.0.0.0".equals(ip)) {
                input.setCountry("");
                input.setProvince("");
                input.setCity("");
                return input;
            }

            try {
                // 查询IP数据库
                String[] result = ipLoader.query(ip);

                if (result != null && result.length >= 3) {
                    input.setCountry(result[0] != null ? result[0] : "");
                    input.setProvince(result[1] != null ? result[1] : "");
                    input.setCity(result[2] != null ? result[2] : "");
                } else {
                    input.setCountry("");
                    input.setProvince("");
                    input.setCity("");
                }

            } catch (Exception e) {
                LOG.error("IP查询失败: {}", ip, e);
                input.setCountry("");
                input.setProvince("");
                input.setCity("");
            }

            return input;

        }).whenComplete((result, throwable) -> {
            if (throwable != null) {
                resultFuture.completeExceptionally(throwable);
            } else {
                resultFuture.complete(Collections.singleton(result));
            }
        });
    }

    @Override
    public void close() throws Exception {
        if (ipLoader != null) {
            ipLoader.close();
            LOG.info("IP数据库加载器已关闭");
        }
    }
}
