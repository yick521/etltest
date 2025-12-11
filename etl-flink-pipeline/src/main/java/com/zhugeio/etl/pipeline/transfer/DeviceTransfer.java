package com.zhugeio.etl.pipeline.transfer;

import com.alibaba.fastjson2.JSONObject;
import com.zhugeio.etl.common.model.output.DeviceRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.regex.Pattern;

/**
 * 设备转换器
 * 
 * 对应 Scala: DeviceTransfer
 * 
 * 处理的事件类型: pl
 */
public class DeviceTransfer implements Serializable {
    
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DeviceTransfer.class);
    
    private static final String NULL_VALUE = "\\N";
    
    // iOS 平台
    private static final int IOS_PLATFORM = 2;
    
    // 分辨率解析正则
    private static final Pattern RESOLUTION_PATTERN = Pattern.compile("[^0-9]");
    
    // 线程安全的日期格式化
    private static final ThreadLocal<SimpleDateFormat> DATE_FORMAT = 
            ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
    
    /**
     * 转换设备数据
     * 
     * @param appId 应用ID
     * @param platform 平台
     * @param pr properties JSON
     * @param ua UserAgent (从父消息继承)
     * @return DeviceRow，如果数据无效返回 null
     */
    public DeviceRow transfer(Integer appId, Integer platform, JSONObject pr, String ua) {
        if (pr == null || appId == null) {
            return null;
        }
        
        // 获取设备ID
        String zgDid = getStringValue(pr, "$zg_did");
        
        if (isNullOrEmpty(zgDid)) {
            return null;
        }
        
        // 获取时间
        Long st = null;
        try {
            // 优先使用 st，否则使用 ct
            JSONObject parent = pr.getJSONObject("_parent");
            if (parent != null) {
                st = parent.getLong("st");
            }
            if (st == null) {
                st = pr.getLong("$ct");
            }
        } catch (Exception e) {
            st = pr.getLong("$ct");
        }
        
        Integer tz = pr.getInteger("$tz");
        
        String time = timestampToDateString(st, tz);
        
        // 解析分辨率
        String resolution = getStringValue(pr, "$rs");
        String[] rsContainer = parseResolution(resolution);
        
        // 获取设备品牌和型号
        String br = ensureLength(getStringValue(pr, "$br"), 256);
        String dv = ensureLength(getStringValue(pr, "$dv"), 256);
        
        // iOS 设备品牌固定为 Apple
        if (platform != null && platform == IOS_PLATFORM) {
            br = "Apple";
        }
        
        // 创建行对象
        DeviceRow row = new DeviceRow(appId);
        row.setDeviceId(zgDid);
        row.setDeviceMd5(ensureLength(getStringValue(pr, "$did"), 256));  // 从父消息
        row.setPlatform(platform);
        row.setDeviceType(NULL_VALUE);
        row.setHorizontalPixel(rsContainer[0]);
        row.setVerticalPixel(rsContainer[1]);
        row.setDeviceBrand(br);
        row.setDeviceModel(dv);
        row.setResolution(ensureLength(resolution, 256));
        row.setPhone(NULL_VALUE);
        row.setImei(ensureLength(getStringValue(pr, "$imei"), 256));
        row.setMac(NULL_VALUE);
        row.setIsPrisonBreak(getStringValue(pr, "$jail"));
        row.setIsCrack(getStringValue(pr, "$private"));
        row.setLanguage(ensureLength(getStringValue(pr, "$lang"), 256));
        row.setTimezone(ensureLength(getStringValue(pr, "$tz"), 256));
        row.setAttr1(ensureLength(getStringValue(pr, "$zs"), 256));  // zgsee 标志
        row.setAttr2(NULL_VALUE);
        row.setAttr3(NULL_VALUE);
        row.setAttr4(NULL_VALUE);
        row.setAttr5(NULL_VALUE);
        
        // 设置更新时间
        if (!isNullOrEmpty(time)) {
            row.setLastUpdateDate(Timestamp.valueOf(time).getTime() / 1000);
        }
        
        return row;
    }
    
    /**
     * 解析分辨率
     * @param resolution 分辨率字符串，如 "1920x1080"
     * @return [宽, 高]
     */
    private String[] parseResolution(String resolution) {
        String[] result = new String[]{NULL_VALUE, NULL_VALUE};
        
        if (isNullOrEmpty(resolution)) {
            return result;
        }
        
        String[] parts = RESOLUTION_PATTERN.split(resolution);
        
        if (parts.length >= 2) {
            result[0] = parts[0];
            result[1] = parts[1];
        }
        
        return result;
    }
    
    /**
     * 时间戳转日期字符串
     */
    private String timestampToDateString(Long ct, Integer tz) {
        if (ct == null) {
            return NULL_VALUE;
        }
        
        // 检查时区范围
        if (tz != null && Math.abs(tz) > 48 * 3600 * 1000) {
            return NULL_VALUE;
        }
        
        try {
            return DATE_FORMAT.get().format(ct);
        } catch (Exception e) {
            return NULL_VALUE;
        }
    }
    
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
