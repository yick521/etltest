package com.zhugeio.etl.pipeline.transfer;

import com.alibaba.fastjson2.JSONObject;
import com.zhugeio.etl.common.model.output.UserPropertyRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * 用户属性转换器
 * 
 * 对应 Scala: UserPropertyTransfer
 * 
 * 处理的事件类型: usr
 * 
 * 支持两种模式:
 * 1. 普通模式: 列顺序为 zg_id, property_id, user_id, property_name, ...
 * 2. CDP 模式: 列顺序为 zg_id, property_id, property_value, user_id, property_name, ...
 */
public class UserPropertyTransfer implements Serializable {
    
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(UserPropertyTransfer.class);
    
    // NULL 值
    private static final String NULL_VALUE = "\\N";
    
    // app_user_id 特殊属性ID
    private static final String APP_USER_ID_PROPERTY_ID = "-1";
    private static final String APP_USER_ID_PROPERTY_NAME = "app_user_id";
    private static final String APP_USER_ID_DATA_TYPE = "string";
    
    // 开启 CDP 的应用ID集合
    private Set<Integer> cdpAppIds;
    
    // 线程安全的日期格式化
    private static final ThreadLocal<SimpleDateFormat> DATE_FORMAT = 
            ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
    
    public UserPropertyTransfer() {
    }
    
    /**
     * 设置开启 CDP 的应用ID集合
     */
    public void setCdpAppIds(Set<Integer> cdpAppIds) {
        this.cdpAppIds = cdpAppIds;
    }
    
    /**
     * 转换用户属性数据
     * 
     * @param appId 应用ID
     * @param platform 平台
     * @param pr properties JSON
     * @return 用户属性行列表 (一个事件可能产生多条属性)
     */
    public List<UserPropertyRow> transfer(Integer appId, Integer platform, JSONObject pr) {
        List<UserPropertyRow> result = new ArrayList<>();
        
        if (pr == null || appId == null) {
            return result;
        }
        
        // 检查核心字段
        String zgId = getStringValue(pr, "$zg_zgid");
        String userId = getStringValue(pr, "$zg_uid");
        
        if (isNullOrEmpty(zgId) || isNullOrEmpty(userId)) {
            return result;
        }
        
        // 检查时间字段
        Long ct = pr.getLong("$ct");
        Integer tz = pr.getInteger("$tz");
        
        if (ct == null || tz == null) {
            return result;
        }
        
        String time = timestampToDateString(ct, tz);
        if (isNullOrEmpty(time)) {
            return result;
        }
        
        long timestamp = Timestamp.valueOf(time).getTime() / 1000;
        
        // 判断是否是 CDP 模式
        boolean cdpMode = cdpAppIds != null && cdpAppIds.contains(appId);
        
        // 处理自定义属性 (以 "_" 开头)
        for (String key : pr.keySet()) {
            if (key.startsWith("_")) {
                UserPropertyRow row = processCustomProperty(appId, platform, pr, key, 
                        zgId, userId, timestamp, cdpMode);
                if (row != null) {
                    result.add(row);
                }
            }
        }
        
        // 处理 app_user_id ($cuid)
        String cuid = getStringValue(pr, "$cuid");
        if (!isNullOrEmpty(cuid)) {
            UserPropertyRow row = processAppUserId(appId, platform, zgId, userId, 
                    cuid, timestamp, cdpMode);
            result.add(row);
        }
        
        return result;
    }
    
    /**
     * 处理自定义属性
     */
    private UserPropertyRow processCustomProperty(Integer appId, Integer platform, JSONObject pr,
                                                   String propKey, String zgId, String userId, 
                                                   long timestamp, boolean cdpMode) {
        
        // 获取属性ID
        String propIdKey = "$zg_upid#" + propKey;
        String propId = getStringValue(pr, propIdKey);
        
        if (isNullOrEmpty(propId)) {
            return null;
        }
        
        // 属性名 (去掉前缀 "_")
        String propName = ensureLength(propKey.substring(1), 256);
        
        // 属性值
        String propValue = ensureLength(getStringValue(pr, propKey), 256);
        
        // 属性类型
        String propTypeKey = "$zg_uptp#" + propKey;
        String propType = ensureLength(getStringValue(pr, propTypeKey), 256);
        
        // 创建行对象
        UserPropertyRow row = new UserPropertyRow(appId, cdpMode);
        row.setZgId(zgId);
        row.setPropertyId(propId);
        row.setUserId(userId);
        row.setPropertyName(propName);
        row.setPropertyDataType(propType);
        row.setPropertyValue(propValue);
        row.setPlatform(platform);
        row.setLastUpdateDate(timestamp);
        
        return row;
    }
    
    /**
     * 处理 app_user_id
     */
    private UserPropertyRow processAppUserId(Integer appId, Integer platform, String zgId, 
                                              String userId, String cuid, long timestamp, 
                                              boolean cdpMode) {
        
        UserPropertyRow row = new UserPropertyRow(appId, cdpMode);
        row.setZgId(zgId);
        row.setPropertyId(APP_USER_ID_PROPERTY_ID);
        row.setUserId(userId);
        row.setPropertyName(APP_USER_ID_PROPERTY_NAME);
        row.setPropertyDataType(APP_USER_ID_DATA_TYPE);
        row.setPropertyValue(ensureLength(cuid, 256));
        row.setPlatform(platform);
        row.setLastUpdateDate(timestamp);
        
        return row;
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
            return DATE_FORMAT.get().format(ct);
        } catch (Exception e) {
            return NULL_VALUE;
        }
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
}
