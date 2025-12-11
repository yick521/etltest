package com.zhugeio.etl.pipeline.operator.dw;

import com.zhugeio.etl.common.util.ip.IpDatabaseLoader;
import com.zhugeio.etl.pipeline.example.ZGMessage;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * IP 地理位置富化算子
 * 
 * 将 ZGMessage 中的 IP 地址解析为地理位置信息
 * 结果写入 message.data 中的 country/province/city 字段
 */
public class IpEnrichOperator extends RichAsyncFunction<ZGMessage, ZGMessage> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(IpEnrichOperator.class);

    private transient IpDatabaseLoader ipLoader;

    private final String hdfsPath;
    private final boolean enableAutoReload;
    private final long reloadIntervalSeconds;
    private final boolean isHdfsHA;

    public IpEnrichOperator(String hdfsPath, boolean enableAutoReload, 
                            long reloadIntervalSeconds, boolean isHdfsHA) {
        this.hdfsPath = hdfsPath;
        this.enableAutoReload = enableAutoReload;
        this.reloadIntervalSeconds = reloadIntervalSeconds;
        this.isHdfsHA = isHdfsHA;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();

        // 初始化 IP 数据库加载器
        ipLoader = new IpDatabaseLoader(
            hdfsPath,
            enableAutoReload,
            reloadIntervalSeconds,
            isHdfsHA
        );
        ipLoader.init();

        LOG.info("[IP富化算子-{}] 初始化成功, 数据库路径: {}", subtaskIndex, hdfsPath);
    }

    @Override
    public void asyncInvoke(ZGMessage message, ResultFuture<ZGMessage> resultFuture) throws Exception {
        
        CompletableFuture.supplyAsync(() -> {
            try {
                // 从 message.data 中获取 IP
                Map<String, Object> data = message.getData();
                if (data == null) {
                    return message;
                }

                Object ipObj = data.get("ip");
                if (ipObj == null) {
                    return message;
                }

                String ip = String.valueOf(ipObj);
                if (ip.isEmpty() || "null".equals(ip) || "0.0.0.0".equals(ip)) {
                    return message;
                }

                // 查询 IP 数据库
                String[] result = ipLoader.query(ip);

                if (result != null && result.length >= 3) {
                    // 将结果写入 data
                    data.put("country", result[0] != null ? result[0] : "");
                    data.put("province", result[1] != null ? result[1] : "");
                    data.put("city", result[2] != null ? result[2] : "");

                    // 如果有第4个元素，可能是 ISP
                    if (result.length >= 4 && result[3] != null) {
                        data.put("isp", result[3]);
                    }
                }

            } catch (Exception e) {
                LOG.error("[IP富化算子] 处理失败: {}", e.getMessage());
            }

            return message;

        }).whenComplete((result, throwable) -> {
            if (throwable != null) {
                LOG.error("[IP富化算子] 异步处理异常", throwable);
                resultFuture.complete(Collections.singleton(message));
            } else {
                resultFuture.complete(Collections.singleton(result));
            }
        });
    }

    @Override
    public void timeout(ZGMessage message, ResultFuture<ZGMessage> resultFuture) throws Exception {
        LOG.warn("[IP富化算子] 处理超时, offset={}", message.getOffset());
        resultFuture.complete(Collections.singleton(message));
    }

    @Override
    public void close() throws Exception {
        if (ipLoader != null) {
            ipLoader.close();
            LOG.info("[IP富化算子] IP 数据库已关闭");
        }
    }
}
