package com.zhugeio.etl.pipeline.util;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * @author ningjh
 * @name ToolsUtil
 * @date 2025/12/4
 * @description
 */
public class Md5Util {

    /**
     * 对字符串进行md5加密，返回固定长度的字符（32位），如：a167d52277c8cec9e5876c10dd43dfe0
     *
     * @param str 需要加密的字符串
     * @return MD5加密后的32位小写字符串
     */
    public static String getMD5Str(String str) {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
            md5.update(bytes, 0, bytes.length);
            String result = new BigInteger(1, md5.digest()).toString(16).toLowerCase();

            // 确保结果是32位，前面补0
            while (result.length() < 32) {
                result = "0" + result;
            }
            return result;

        } catch (NoSuchAlgorithmException e) {
            // 通常不会发生，因为MD5是标准算法
            throw new RuntimeException("MD5 algorithm not found", e);
        }
    }

}
