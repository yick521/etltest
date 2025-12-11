package com.zhugeio.etl.pipeline.operator.dw;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.zhugeio.etl.common.model.output.*;
import com.zhugeio.etl.pipeline.config.Config;
import com.zhugeio.etl.pipeline.service.BaiduKeywordService;
import com.zhugeio.etl.pipeline.service.EventAttrColumnService;
import com.zhugeio.etl.pipeline.transfer.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 数据路由算子
 * 
 * 对应 Scala: MsgTransfer.transfer
 * 
 * 输入: 已富化的 ZGMessage JSON (包含 IP/UA 解析结果)
 * 输出: 根据 dt 类型路由到不同的侧输出
 * 
 * 路由规则:
 * - zgid -> UserRow
 * - pl -> DeviceRow
 * - usr -> UserPropertyRow (可能多条)
 * - evt/vtl/mkt/ss/se/abp -> EventAttrRow
 * 
 * 注意: 不再输出 EventAllRow (Scala 工程中可配置关闭)
 */
public class DataRouterOperator extends ProcessFunction<String, EventAttrRow> {
    
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DataRouterOperator.class);
    
    // 侧输出标签
    public static final OutputTag<UserRow> USER_OUTPUT = 
            new OutputTag<UserRow>("user-output") {};
    public static final OutputTag<DeviceRow> DEVICE_OUTPUT = 
            new OutputTag<DeviceRow>("device-output") {};
    public static final OutputTag<UserPropertyRow> USER_PROPERTY_OUTPUT = 
            new OutputTag<UserPropertyRow>("user-property-output") {};
    
    // 黑名单应用
    private Set<String> blackAppIds;
    
    // 白名单应用 (用于百度关键词)
    private Set<String> whiteAppIds;
    
    // CDP 应用
    private Set<Integer> cdpAppIds;
    
    // 转换器
    private transient UserTransfer userTransfer;
    private transient DeviceTransfer deviceTransfer;
    private transient UserPropertyTransfer userPropertyTransfer;
    private transient EventAttrTransfer eventAttrTransfer;
    
    // 服务
    private transient EventAttrColumnService columnService;
    private transient BaiduKeywordService keywordService;
    
    // 统计
    private final AtomicLong userCount = new AtomicLong(0);
    private final AtomicLong deviceCount = new AtomicLong(0);
    private final AtomicLong userPropertyCount = new AtomicLong(0);
    private final AtomicLong eventAttrCount = new AtomicLong(0);
    private final AtomicLong unknownCount = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);
    private final AtomicLong blacklistedCount = new AtomicLong(0);
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        // 加载黑名单
        String blackAppsStr = Config.getString(Config.BLACK_APPIDS, "-1");
        blackAppIds = new HashSet<>(Arrays.asList(blackAppsStr.split(",")));
        
        // 加载白名单 (用于百度关键词)
        String whiteAppsStr = Config.getString(Config.WHITE_APPID, "");
        whiteAppIds = new HashSet<>();
        if (!whiteAppsStr.isEmpty()) {
            whiteAppIds.addAll(Arrays.asList(whiteAppsStr.split(",")));
        }
        
        // CDP 应用 (TODO: 从配置或数据库加载)
        cdpAppIds = new HashSet<>();
        
        // 初始化列映射服务
        columnService = new EventAttrColumnService(
                Config.getString(Config.KVROCKS_HOST, "localhost"),
                Config.getInt(Config.KVROCKS_PORT, 6379),
                Config.getBoolean(Config.KVROCKS_CLUSTER, false),
                Config.getInt(Config.KVROCKS_LOCAL_CACHE_SIZE, 10000),
                Config.getInt(Config.KVROCKS_LOCAL_CACHE_EXPIRE_MINUTES, 60)
        );
        columnService.init();
        
        // 初始化百度关键词服务
        keywordService = new BaiduKeywordService(
                Config.getString(Config.BAIDU_URL, "http://referer.bj.baidubce.com/v1/eqid"),
                Config.getString(Config.BAIDU_ID, ""),
                Config.getString(Config.BAIDU_KEY, ""),
                Config.getString(Config.REDIS_HOST, "localhost"),
                Config.getInt(Config.REDIS_PORT, 6379),
                Config.getBoolean(Config.REDIS_CLUSTER, false),
                Config.getInt(Config.REQUEST_TIMEOUT, 5)
        );
        keywordService.init();
        
        // 初始化转换器
        userTransfer = new UserTransfer();
        deviceTransfer = new DeviceTransfer();
        
        userPropertyTransfer = new UserPropertyTransfer();
        userPropertyTransfer.setCdpAppIds(cdpAppIds);
        
        int expireSubDays = Config.getInt(Config.TIME_EXPIRE_SUBDAYS, 7);
        int expireAddDays = Config.getInt(Config.TIME_EXPIRE_ADDDAYS, 1);
        eventAttrTransfer = new EventAttrTransfer(columnService, expireSubDays, expireAddDays);
        
        LOG.info("DataRouterOperator initialized: blackAppIds={}, whiteAppIds={}", 
                blackAppIds.size(), whiteAppIds.size());
    }
    
    @Override
    public void processElement(String value, Context ctx, Collector<EventAttrRow> out) throws Exception {
        try {
            JSONObject msgJson = JSONObject.parseObject(value);
            if (msgJson == null) {
                errorCount.incrementAndGet();
                return;
            }
            
            // 获取基础信息
            Integer appId = msgJson.getInteger("app_id");
            Integer platform = msgJson.getInteger("plat");
            String ua = msgJson.getString("ua");
            String ip = msgJson.getString("ip");
            
            if (appId == null) {
                errorCount.incrementAndGet();
                return;
            }
            
            // 黑名单检查
            if (blackAppIds.contains(String.valueOf(appId))) {
                blacklistedCount.incrementAndGet();
                return;
            }
            
            // 获取富化数据 (由上游算子填充)
            String[] ipResult = extractIpResult(msgJson);
            Map<String, String> uaResult = extractUaResult(msgJson);
            String business = extractBusiness(msgJson);
            
            // 处理 data 数组
            JSONArray dataArray = msgJson.getJSONArray("data");
            if (dataArray == null || dataArray.isEmpty()) {
                return;
            }
            
            // 预先提取所有 eqid 用于批量查询关键词
            Map<String, String> eqidKeywords = preloadKeywords(appId, dataArray);
            
            // 处理每个数据项
            for (int i = 0; i < dataArray.size(); i++) {
                JSONObject dataItem = dataArray.getJSONObject(i);
                if (dataItem == null) {
                    continue;
                }
                
                String dt = dataItem.getString("dt");
                JSONObject pr = dataItem.getJSONObject("pr");
                
                if (dt == null || pr == null) {
                    continue;
                }
                
                // 根据 dt 类型路由
                routeByDt(ctx, out, dt, appId, platform, pr, ip, ipResult, ua, uaResult, 
                          business, eqidKeywords);
            }
            
        } catch (Exception e) {
            errorCount.incrementAndGet();
            LOG.error("Error processing message", e);
        }
    }
    
    /**
     * 根据 dt 类型路由数据
     */
    private void routeByDt(Context ctx, Collector<EventAttrRow> out, String dt,
                           Integer appId, Integer platform, JSONObject pr,
                           String ip, String[] ipResult, String ua, Map<String, String> uaResult,
                           String business, Map<String, String> eqidKeywords) {
        
        switch (dt) {
            case "zgid":
                // 用户映射表
                UserRow userRow = userTransfer.transfer(appId, platform, pr);
                if (userRow != null) {
                    ctx.output(USER_OUTPUT, userRow);
                    userCount.incrementAndGet();
                }
                break;
                
            case "pl":
                // 设备表
                DeviceRow deviceRow = deviceTransfer.transfer(appId, platform, pr);
                if (deviceRow != null) {
                    ctx.output(DEVICE_OUTPUT, deviceRow);
                    deviceCount.incrementAndGet();
                }
                break;
                
            case "usr":
                // 用户属性表 (可能产生多条)
                List<UserPropertyRow> propRows = userPropertyTransfer.transfer(appId, platform, pr);
                for (UserPropertyRow propRow : propRows) {
                    ctx.output(USER_PROPERTY_OUTPUT, propRow);
                    userPropertyCount.incrementAndGet();
                }
                break;
                
            case "evt":
            case "vtl":
            case "mkt":
            case "ss":
            case "se":
            case "abp":
                // 事件属性表
                EventAttrRow eventRow = eventAttrTransfer.transfer(
                        appId, platform, dt, pr, ip, ipResult, ua, uaResult, business, eqidKeywords);
                if (eventRow != null) {
                    out.collect(eventRow);
                    eventAttrCount.incrementAndGet();
                }
                break;
                
            default:
                unknownCount.incrementAndGet();
                break;
        }
    }
    
    /**
     * 预加载百度关键词
     */
    private Map<String, String> preloadKeywords(Integer appId, JSONArray dataArray) {
        Map<String, String> result = new HashMap<>();
        
        // 只对白名单应用启用
        if (!whiteAppIds.contains(String.valueOf(appId))) {
            return result;
        }
        
        Set<String> eqids = new HashSet<>();
        
        for (int i = 0; i < dataArray.size(); i++) {
            JSONObject dataItem = dataArray.getJSONObject(i);
            if (dataItem == null) {
                continue;
            }
            
            String dt = dataItem.getString("dt");
            if (!keywordService.shouldExtractKeyword(dt)) {
                continue;
            }
            
            JSONObject pr = dataItem.getJSONObject("pr");
            if (pr == null) {
                continue;
            }
            
            // 检查是否已有 utm_term
            String utmTerm = pr.getString("$utm_term");
            if (utmTerm != null && !utmTerm.isEmpty() && !"\\N".equals(utmTerm)) {
                continue;
            }
            
            // 提取 eqid
            String ref = pr.getString("$ref");
            String eqid = keywordService.extractEqid(ref);
            if (eqid != null) {
                eqids.add(eqid);
                // 回写 eqid 到 pr
                pr.put("$eqid", eqid);
            }
        }
        
        // 批量查询
        if (!eqids.isEmpty()) {
            try {
                result = keywordService.preloadKeywordsAsync(eqids).get();
            } catch (Exception e) {
                LOG.warn("Failed to preload keywords", e);
            }
        }
        
        return result;
    }
    
    /**
     * 提取 IP 解析结果
     */
    private String[] extractIpResult(JSONObject msgJson) {
        JSONObject enriched = msgJson.getJSONObject("_enriched");
        if (enriched == null) {
            return new String[]{"\\N", "\\N", "\\N"};
        }
        
        JSONObject ipInfo = enriched.getJSONObject("ip");
        if (ipInfo == null) {
            return new String[]{"\\N", "\\N", "\\N"};
        }
        
        return new String[]{
                ipInfo.getString("country"),
                ipInfo.getString("province"),
                ipInfo.getString("city")
        };
    }
    
    /**
     * 提取 UA 解析结果
     */
    private Map<String, String> extractUaResult(JSONObject msgJson) {
        Map<String, String> result = new HashMap<>();
        
        JSONObject enriched = msgJson.getJSONObject("_enriched");
        if (enriched == null) {
            return result;
        }
        
        JSONObject uaInfo = enriched.getJSONObject("ua");
        if (uaInfo == null) {
            return result;
        }
        
        result.put("os", uaInfo.getString("os"));
        result.put("os_version", uaInfo.getString("os_version"));
        result.put("browser", uaInfo.getString("browser"));
        result.put("browser_version", uaInfo.getString("browser_version"));
        
        return result;
    }
    
    /**
     * 提取业务标识
     */
    private String extractBusiness(JSONObject msgJson) {
        String business = msgJson.getString("business");
        return business != null ? business : "\\N";
    }
    
    @Override
    public void close() throws Exception {
        LOG.info("Closing DataRouterOperator - Stats: user={}, device={}, userProperty={}, " +
                 "eventAttr={}, unknown={}, error={}, blacklisted={}",
                userCount.get(), deviceCount.get(), userPropertyCount.get(),
                eventAttrCount.get(), unknownCount.get(), errorCount.get(), blacklistedCount.get());
        
        if (columnService != null) {
            columnService.close();
        }
        
        if (keywordService != null) {
            keywordService.close();
        }
        
        super.close();
    }
    
    // Getters for stats
    public long getUserCount() { return userCount.get(); }
    public long getDeviceCount() { return deviceCount.get(); }
    public long getUserPropertyCount() { return userPropertyCount.get(); }
    public long getEventAttrCount() { return eventAttrCount.get(); }
    public long getUnknownCount() { return unknownCount.get(); }
    public long getErrorCount() { return errorCount.get(); }
}
