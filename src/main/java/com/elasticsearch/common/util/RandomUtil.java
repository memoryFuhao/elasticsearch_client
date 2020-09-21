package com.elasticsearch.common.util;

import java.util.Random;

public class RandomUtil {
    
    private static Random r = new Random();
    
    /**
     * 获取随机数字符串
     *
     * @param bound 随机范围
     */
    public static String getStr(int bound) {
        return String.valueOf(r.nextInt(bound));
    }
    
    /**
     * 获取随机数int数值
     *
     * @param bound 随机范围
     */
    public static int getInt(int bound) {
        return r.nextInt(bound);
    }
    
    /**
     * 获取随机数long数值
     */
    public static long getLong() {
        return r.nextLong();
    }
}
