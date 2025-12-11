package com.zhugeio.etl.pipeline.transfer;

import com.alibaba.fastjson2.JSONObject;
import com.zhugeio.etl.common.model.output.EventAttrRow;
import com.zhugeio.etl.pipeline.service.EventAttrColumnService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * 事件属性转换器
 * 
 * 对应 Scala: EventAttrTransfer
 * 
 * 处理的事件类型: evt, vtl, mkt, ss, se, abp
 * 
 * 注意: 只有 evt, mkt, abp 类型有自定义属性
 * - evt: 以 "_" 开头的属性
 * - mkt: 从 SdkConfig.getMktAttrs 获取
 * - abp: 从 SdkConfig.getAbpAttrs 获取
 * - ss, se, vtl: 没有自定义属性
 */
public class EventAttrTransfer implements Serializable {
    
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(EventAttrTransfer.class);
    
    // NULL 值
    private static final String NULL_VALUE = "\\N";
    
    // 平台类型
    private static final int JS_SDK = 3;
    private static final int WXA_SDK = 4;
    
    // 搜索引擎域名
    private static final String WWW_BAIDU_COM = ".baidu.com";
    private static final String WWW_SOGOU_COM = ".sogou.com";
    private static final String CN_BING_COM = ".bing.com";
    private static final String WWW_SO_COM = ".so.com";
    private static final String WWW_GOOGLE_COM = ".google.com";
    private static final String WWW_GOOGLE_CO = ".google.co";
    private static final String M_SM_CN = "m.sm.cn";
    
    // 有自定义属性的事件类型
    private static final Set<String> CUSTOM_PROPERTY_DT = new HashSet<>(Arrays.asList("evt", "mkt", "abp"));
    
    // 时间过期配置
    private final int expireSubDays;
    private final int expireAddDays;
    
    // 列映射服务
    private final EventAttrColumnService columnService;
    
    // mkt 和 abp 的属性集合 (需要从配置加载)
    private Set<String> mktAttrs;
    private Set<String> abpAttrs;
    
    // 线程安全的日期格式化
    private static final ThreadLocal<SimpleDateFormat> DATE_FORMAT = 
            ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
    private static final ThreadLocal<SimpleDateFormat> DAY_FORMAT = 
            ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMdd"));
    private static final ThreadLocal<SimpleDateFormat> YEAR_WEEK_FORMAT = 
            ThreadLocal.withInitial(() -> new SimpleDateFormat("YYYYww"));
    
    public EventAttrTransfer(EventAttrColumnService columnService, int expireSubDays, int expireAddDays) {
        this.columnService = columnService;
        this.expireSubDays = expireSubDays;
        this.expireAddDays = expireAddDays;
        
        // 初始化 mkt 和 abp 属性集合 (TODO: 从配置加载)
        this.mktAttrs = new HashSet<>(Arrays.asList(
                "ct", "tz", "zg_zgid", "zg_did", "zg_eid", "zg_sid", "zg_uid", "eid", "uuid"
        ));
        this.abpAttrs = new HashSet<>(Arrays.asList(
                "ct", "tz", "zg_zgid", "zg_did", "zg_eid", "zg_sid", "zg_uid", "eid", "uuid"
        ));
    }
    
    /**
     * 设置 mkt 属性集合
     */
    public void setMktAttrs(Set<String> mktAttrs) {
        this.mktAttrs = mktAttrs;
    }
    
    /**
     * 设置 abp 属性集合
     */
    public void setAbpAttrs(Set<String> abpAttrs) {
        this.abpAttrs = abpAttrs;
    }
    
    /**
     * 转换事件数据为 EventAttrRow
     * 
     * @param appId 应用ID
     * @param platform 平台
     * @param dt 数据类型 (evt/vtl/mkt/ss/se/abp)
     * @param pr properties JSON
     * @param ip IP地址
     * @param ipResult IP解析结果 [国家, 省份, 城市]
     * @param ua UserAgent
     * @param uaResult UA解析结果 {os, os_version, browser, browser_version}
     * @param business 业务标识
     * @param eqidKeywords 百度关键词映射
     * @return EventAttrRow，如果数据无效返回 null
     */
    public EventAttrRow transfer(Integer appId, Integer platform, String dt, JSONObject pr,
                                  String ip, String[] ipResult, String ua, Map<String, String> uaResult,
                                  String business, Map<String, String> eqidKeywords) {
        
        // 1. 校验核心字段
        String zgId = getStringValue(pr, "$zg_zgid");
        String zgEid = getStringValue(pr, "$zg_eid");
        String zgDid = getStringValue(pr, "$zg_did");
        
        if (isNullOrEmpty(zgId)) {
            LOG.debug("Missing $zg_zgid");
            return null;
        }
        if (isNullOrEmpty(zgEid)) {
            LOG.debug("Missing $zg_eid");
            return null;
        }
        if (isNullOrEmpty(zgDid)) {
            LOG.debug("Missing $zg_did");
            return null;
        }
        
        // 2. 校验时间字段
        Long ct = pr.getLong("$ct");
        Integer tz = pr.getInteger("$tz");
        
        if (ct == null || tz == null) {
            LOG.debug("Missing $ct or $tz");
            return null;
        }
        
        String realtime = timestampToDateString(ct, tz);
        if (isNullOrEmpty(realtime)) {
            LOG.debug("Invalid timestamp");
            return null;
        }
        
        // 3. 检查时间是否过期
        if (isExpiredTime(realtime)) {
            LOG.debug("Event time expired: {}", realtime);
            return null;
        }
        
        // 4. 解析时间组件
        Map<String, String> timeComponents = getTimeComponents(ct, tz);
        if (timeComponents.isEmpty()) {
            return null;
        }
        
        // 5. 创建行对象
        EventAttrRow row = new EventAttrRow(appId);
        
        // 6. 填充基础字段
        fillBasicFields(row, pr, platform, dt, ip, ipResult, ua, uaResult, 
                        business, eqidKeywords, realtime, timeComponents);
        
        // 7. 填充自定义属性 (只有 evt, mkt, abp)
        if (CUSTOM_PROPERTY_DT.contains(dt)) {
            fillCustomProperties(row, pr, dt, zgEid);
        }
        
        // 8. 设置分区字段
        row.setEid(zgEid);
        row.setYw(getYearWeek(realtime));
        
        return row;
    }
    
    /**
     * 异步转换 (预加载属性列映射)
     */
    public CompletableFuture<EventAttrRow> transferAsync(Integer appId, Integer platform, String dt, 
                                                          JSONObject pr, String ip, String[] ipResult, 
                                                          String ua, Map<String, String> uaResult,
                                                          String business, Map<String, String> eqidKeywords) {
        
        // 如果不是需要自定义属性的类型，直接同步处理
        if (!CUSTOM_PROPERTY_DT.contains(dt)) {
            return CompletableFuture.completedFuture(
                    transfer(appId, platform, dt, pr, ip, ipResult, ua, uaResult, business, eqidKeywords));
        }
        
        // 预加载自定义属性的列映射
        String zgEid = getStringValue(pr, "$zg_eid");
        List<String[]> pairs = extractPropertyPairs(pr, dt, zgEid);
        
        if (pairs.isEmpty()) {
            return CompletableFuture.completedFuture(
                    transfer(appId, platform, dt, pr, ip, ipResult, ua, uaResult, business, eqidKeywords));
        }
        
        // 异步预加载后执行转换
        return columnService.preloadAsync(pairs)
                .thenApply(v -> transfer(appId, platform, dt, pr, ip, ipResult, ua, uaResult, business, eqidKeywords));
    }
    
    /**
     * 填充基础字段 (列 0-39)
     */
    private void fillBasicFields(EventAttrRow row, JSONObject pr, Integer platform, String dt,
                                  String ip, String[] ipResult, String ua, Map<String, String> uaResult,
                                  String business, Map<String, String> eqidKeywords,
                                  String realtime, Map<String, String> timeComponents) {
        
        // 列 0-3: 核心ID
        row.setZgId(getStringValue(pr, "$zg_zgid"));
        row.setSessionId(getStringValue(pr, "$zg_sid"));
        row.setUuid(ensureLength(getStringValue(pr, "$uuid"), 256));
        row.setEventIdColumn(getStringValue(pr, "$zg_eid"));
        
        // 列 4-6: 时间字段 (Doris 顺序: day, date, time)
        row.setBeginDayId(timeComponents.get("day"));
        row.setBeginDate(String.valueOf(Timestamp.valueOf(realtime).getTime() / 1000));
        row.setBeginTimeId(timeComponents.get("hour") + "0" + timeComponents.get("mill"));
        
        // 列 7-9: 用户/设备/事件
        row.setDeviceId(getStringValue(pr, "$zg_did"));
        row.setUserId(getStringValue(pr, "$zg_uid"));
        row.setEventName(ensureLength(getEventName(pr, dt), 256));
        
        // 列 10-13: 平台/网络
        row.setPlatform(platform);
        row.setNetwork(ensureNetwork(getStringValue(pr, "$net")));
        row.setMccmnc(ensureIntLength(getStringValue(pr, "$cr"), 256));
        row.setUseragent(ensureLength(ua, 256));
        
        // 列 14-16: URL
        String website = ensureLength(getStringValue(pr, "$referrer_domain"), 1088);
        String currentUrl = ensureLength(getStringValue(pr, "$url"), 1088);
        String referrerUrl = ensureLength(getStringValue(pr, "$ref"), 1088);
        row.setWebsite(website);
        row.setCurrentUrl(currentUrl);
        row.setReferrerUrl(referrerUrl);
        
        // 列 17-18: 渠道/版本
        row.setChannel(ensureLength(getStringValue(pr, "$cn"), 256));
        row.setAppVersion(ensureLength(getStringValue(pr, "$vn"), 256));
        
        // 列 19-23: IP/地理位置
        row.setIp(ipToLong(ip));
        row.setIpStr(ip);
        row.setCountry(ipResult != null && ipResult.length > 0 ? nullIfEmpty(ipResult[0]) : NULL_VALUE);
        row.setArea(ipResult != null && ipResult.length > 1 ? nullIfEmpty(ipResult[1]) : NULL_VALUE);
        row.setCity(ipResult != null && ipResult.length > 2 ? nullIfEmpty(ipResult[2]) : NULL_VALUE);
        
        // 列 24-27: 操作系统/浏览器
        fillOsBrowser(row, pr, platform, uaResult);
        
        // 列 28-32: UTM 参数
        fillUtmParams(row, pr, website, referrerUrl, eqidKeywords);
        
        // 列 33-34: 持续时间/UTC时间
        row.setDuration(ensureIntRange(getStringValue(pr, "$dru"), 0, 86400000));
        row.setUtcDate(String.valueOf(System.currentTimeMillis()));
        
        // 列 35-39: 扩展属性
        row.setAttr1(business);
        row.setAttr2(ensureLength(getStringValue(pr, "$wxeid"), 256));
        row.setAttr3(ensureLength(getStringValue(pr, "$env_attr3"), 256));
        row.setAttr4(NULL_VALUE);
        row.setAttr5(getStringValue(pr, "$zg_zgid") + "_" + getStringValue(pr, "$zg_sid"));
    }
    
    /**
     * 填充操作系统和浏览器字段
     */
    private void fillOsBrowser(EventAttrRow row, JSONObject pr, Integer platform, Map<String, String> uaResult) {
        String os, ov, browser, bv;
        
        if (platform != null && platform != JS_SDK && platform != WXA_SDK) {
            // 非 Web 平台，从 pr 获取
            os = ensureLength(getStringValue(pr, "$os"), 256);
            ov = ensureLength(getIntVersion(getStringValue(pr, "$ov")), 9);
        } else {
            // Web 平台，从 UA 解析结果获取
            os = uaResult != null ? uaResult.getOrDefault("os", NULL_VALUE) : NULL_VALUE;
            ov = uaResult != null ? ensureLength(uaResult.getOrDefault("os_version", NULL_VALUE), 9) : NULL_VALUE;
        }
        
        browser = uaResult != null ? uaResult.getOrDefault("browser", NULL_VALUE) : NULL_VALUE;
        bv = uaResult != null ? ensureLength(uaResult.getOrDefault("browser_version", NULL_VALUE), 9) : NULL_VALUE;
        
        row.setOs(os);
        row.setOv(ov);
        row.setBs(browser);
        row.setBv(bv);
    }
    
    /**
     * 填充 UTM 参数
     */
    private void fillUtmParams(EventAttrRow row, JSONObject pr, String website, 
                                String referrerUrl, Map<String, String> eqidKeywords) {
        
        String source = ensureLength(getStringValue(pr, "$utm_source"), 256);
        String medium = ensureLength(getStringValue(pr, "$utm_medium"), 256);
        String campaign = ensureLength(getStringValue(pr, "$utm_campaign"), 256);
        String content = ensureLength(getStringValue(pr, "$utm_content"), 256);
        String term = getStringValue(pr, "$utm_term");
        String eqid = getStringValue(pr, "$eqid");
        
        // 如果没有 UTM 参数，根据来源网站推断
        if (isNullOrEmpty(source) && isNullOrEmpty(medium) && isNullOrEmpty(campaign) 
                && isNullOrEmpty(content) && isNullOrEmpty(term)) {
            
            if (isSearchEngine(website)) {
                medium = "搜索自然流量";
                source = website;
                term = getSearchKeyword(eqid, term, referrerUrl, eqidKeywords);
            } else if (!isNullOrEmpty(referrerUrl)) {
                medium = "引荐";
                source = website;
                term = getSearchKeyword(eqid, term, referrerUrl, eqidKeywords);
            }
        } else {
            if (isNullOrEmpty(source)) {
                source = website;
            }
        }
        
        row.setUtmSource(source);
        row.setUtmMedium(medium);
        row.setUtmCampaign(campaign);
        row.setUtmContent(content);
        row.setUtmTerm(ensureLength(term, 256));
    }
    
    /**
     * 填充自定义属性 (列 40-139) 和类型 (列 140-239)
     * 
     * 只有 evt, mkt, abp 类型有自定义属性
     */
    private void fillCustomProperties(EventAttrRow row, JSONObject pr, String dt, String zgEid) {
        List<String> properties = getCustomPropertyKeys(pr, dt);
        
        for (String propName : properties) {
            String propIdKey = "$zg_epid#" + propName;
            String propId = getStringValue(pr, propIdKey);
            
            if (!isNullOrEmpty(propId)) {
                // 获取列索引
                Integer colIndex = columnService.getColumnIndex(zgEid, propId);
                
                if (colIndex != null && colIndex >= 1 && colIndex <= 100) {
                    // 设置属性值
                    String propValue = ensureLength(getStringValue(pr, propName), 256);
                    row.setCustomProperty(colIndex, propValue);
                    
                    // 设置属性类型
                    String propTypeKey = "$zg_eptp#" + propName;
                    String propType = ensureLength(getStringValue(pr, propTypeKey), 256);
                    if (!isNullOrEmpty(propType)) {
                        row.setPropertyType(colIndex, propType);
                    }
                }
            }
        }
    }
    
    /**
     * 获取自定义属性键列表
     */
    private List<String> getCustomPropertyKeys(JSONObject pr, String dt) {
        List<String> properties = new ArrayList<>();
        
        if (pr == null) {
            return properties;
        }
        
        Set<String> keys = pr.keySet();
        
        switch (dt) {
            case "evt":
                // evt: 以 "_" 开头的属性
                for (String key : keys) {
                    if (key.startsWith("_")) {
                        properties.add(key);
                    }
                }
                break;
                
            case "mkt":
                // mkt: 从配置的属性集合中获取
                for (String key : keys) {
                    if (key.startsWith("$") && mktAttrs.contains(key.substring(1))) {
                        properties.add(key);
                    } else if (key.startsWith("_")) {
                        properties.add(key);
                    }
                }
                break;
                
            case "abp":
                // abp: 从配置的属性集合中获取
                for (String key : keys) {
                    if (key.startsWith("$") && abpAttrs.contains(key.substring(1))) {
                        properties.add(key);
                    } else if (key.startsWith("_")) {
                        properties.add(key);
                    }
                }
                break;
                
            default:
                // ss, se, vtl: 没有自定义属性
                break;
        }
        
        return properties;
    }
    
    /**
     * 提取属性ID对，用于预加载
     */
    private List<String[]> extractPropertyPairs(JSONObject pr, String dt, String zgEid) {
        List<String[]> pairs = new ArrayList<>();
        List<String> properties = getCustomPropertyKeys(pr, dt);
        
        for (String propName : properties) {
            String propIdKey = "$zg_epid#" + propName;
            String propId = getStringValue(pr, propIdKey);
            if (!isNullOrEmpty(propId)) {
                pairs.add(new String[]{zgEid, propId});
            }
        }
        
        return pairs;
    }
    
    /**
     * 获取事件名称
     */
    private String getEventName(JSONObject pr, String dt) {
        String zgEid = getStringValue(pr, "$zg_eid");
        
        // ss 和 se 有特殊事件名
        if ("-1".equals(zgEid)) {
            return "st";
        } else if ("-2".equals(zgEid)) {
            return "se";
        }
        
        return getStringValue(pr, "$eid");
    }
    
    /**
     * 时间戳转日期字符串
     */
    private String timestampToDateString(Long ct, Integer tz) {
        if (ct == null || tz == null) {
            return NULL_VALUE;
        }
        
        // 检查时区范围
        if (Math.abs(tz) > 48 * 3600 * 1000) {
            return NULL_VALUE;
        }
        
        try {
            Date date = new Date(ct);
            return DATE_FORMAT.get().format(date);
        } catch (Exception e) {
            return NULL_VALUE;
        }
    }
    
    /**
     * 获取时间组件
     */
    private Map<String, String> getTimeComponents(Long ct, Integer tz) {
        Map<String, String> result = new HashMap<>();
        
        try {
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(ct);
            
            result.put("day", DAY_FORMAT.get().format(cal.getTime()));
            result.put("hour", String.format("%02d", cal.get(Calendar.HOUR_OF_DAY)));
            result.put("mill", String.format("%03d", cal.get(Calendar.MILLISECOND)));
            
        } catch (Exception e) {
            LOG.warn("Failed to get time components", e);
        }
        
        return result;
    }
    
    /**
     * 计算年周 (减1天后计算)
     */
    private String getYearWeek(String realtime) {
        try {
            Date date = DATE_FORMAT.get().parse(realtime);
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            cal.add(Calendar.DAY_OF_MONTH, -1);  // 减1天
            return YEAR_WEEK_FORMAT.get().format(cal.getTime());
        } catch (Exception e) {
            return NULL_VALUE;
        }
    }
    
    /**
     * 检查时间是否过期
     */
    private boolean isExpiredTime(String realtime) {
        try {
            Date eventDate = DATE_FORMAT.get().parse(realtime);
            long eventTime = eventDate.getTime();
            
            Calendar cal = Calendar.getInstance();
            
            // 计算允许的最大时间
            cal.add(Calendar.DAY_OF_MONTH, expireAddDays);
            cal.set(Calendar.HOUR_OF_DAY, 23);
            cal.set(Calendar.MINUTE, 59);
            cal.set(Calendar.SECOND, 59);
            cal.set(Calendar.MILLISECOND, 999);
            long maxTime = cal.getTimeInMillis();
            
            // 计算允许的最小时间
            cal.setTime(new Date());
            cal.add(Calendar.DAY_OF_MONTH, -expireSubDays);
            cal.set(Calendar.HOUR_OF_DAY, 0);
            cal.set(Calendar.MINUTE, 0);
            cal.set(Calendar.SECOND, 0);
            cal.set(Calendar.MILLISECOND, 0);
            long minTime = cal.getTimeInMillis();
            
            return eventTime < minTime || eventTime > maxTime;
            
        } catch (Exception e) {
            return true;
        }
    }
    
    /**
     * 判断是否是搜索引擎
     */
    private boolean isSearchEngine(String website) {
        if (isNullOrEmpty(website)) {
            return false;
        }
        return website.contains(WWW_BAIDU_COM) || website.contains(WWW_SOGOU_COM) ||
               website.contains(CN_BING_COM) || website.contains(WWW_SO_COM) ||
               website.contains(M_SM_CN) || website.contains(WWW_GOOGLE_COM) ||
               website.contains(WWW_GOOGLE_CO);
    }
    
    /**
     * 获取搜索关键词
     */
    private String getSearchKeyword(String eqid, String term, String referrerUrl, 
                                     Map<String, String> eqidKeywords) {
        if (!isNullOrEmpty(term)) {
            return term;
        }
        
        // 从 eqid 映射获取
        if (!isNullOrEmpty(eqid) && eqidKeywords != null) {
            String keyword = eqidKeywords.get(eqid);
            if (!isNullOrEmpty(keyword)) {
                return keyword;
            }
        }
        
        // TODO: 从 URL 参数解析关键词
        return NULL_VALUE;
    }
    
    // ============ 工具方法 ============
    
    private String getStringValue(JSONObject json, String key) {
        if (json == null || key == null) {
            return NULL_VALUE;
        }
        String value = json.getString(key);
        return value != null ? value : NULL_VALUE;
    }
    
    private boolean isNullOrEmpty(String value) {
        return value == null || value.isEmpty() || NULL_VALUE.equals(value);
    }
    
    private String nullIfEmpty(String value) {
        return (value == null || value.isEmpty()) ? NULL_VALUE : value;
    }
    
    private String ensureLength(String value, int maxLength) {
        if (isNullOrEmpty(value)) {
            return NULL_VALUE;
        }
        value = value.replaceAll("[\t\n\r\"\\\\\u0000]", " ").trim();
        if (value.length() > maxLength) {
            return value.substring(0, maxLength);
        }
        return value;
    }
    
    private String ensureNetwork(String value) {
        if ("-1".equals(value)) {
            return NULL_VALUE;
        }
        return ensureIntLength(value, 256);
    }
    
    private String ensureIntLength(String value, int maxLength) {
        if (isNullOrEmpty(value) || "null".equals(value) || "(null)(null)".equals(value)) {
            return NULL_VALUE;
        }
        if (value.length() > 6) {
            return NULL_VALUE;
        }
        if (!value.matches("[0-9]*")) {
            return NULL_VALUE;
        }
        return value;
    }
    
    private String ensureIntRange(String value, int min, int max) {
        if (isNullOrEmpty(value)) {
            return NULL_VALUE;
        }
        try {
            int intValue = Integer.parseInt(value);
            if (intValue >= min && intValue <= max) {
                return value;
            }
        } catch (NumberFormatException e) {
            // ignore
        }
        return NULL_VALUE;
    }
    
    private String getIntVersion(String value) {
        if (isNullOrEmpty(value)) {
            return NULL_VALUE;
        }
        java.util.regex.Matcher matcher = java.util.regex.Pattern.compile("^([0-9]+)\\.*").matcher(value.trim());
        if (matcher.find()) {
            return matcher.group(1);
        }
        return NULL_VALUE;
    }
    
    private Long ipToLong(String ip) {
        if (isNullOrEmpty(ip)) {
            return null;
        }
        try {
            String[] parts = ip.split("\\.");
            if (parts.length != 4) {
                return null;
            }
            long result = 0;
            for (int i = 0; i < 4; i++) {
                result = (result << 8) | Integer.parseInt(parts[i]);
            }
            return result;
        } catch (Exception e) {
            return null;
        }
    }
}
