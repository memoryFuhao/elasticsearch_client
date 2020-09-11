package com.elasticsearch.common.util;

import java.util.Random;

public class RandomUtil {
    
    /**
     * 获取随机数字符串
     *
     * @param bound 随机范围
     */
    public static String getStr(int bound) {
        Random r = new Random();
        return String.valueOf(r.nextInt(bound));
    }
    
    /**
     * 获取随机数int数值
     *
     * @param bound 随机范围
     */
    public static int getInt(int bound) {
        Random r = new Random();
        return r.nextInt(bound);
    }
    
    /**
     * 获取随机数long数值
     */
    public static long getLong() {
        Random r = new Random();
        return r.nextLong();
    }
    
    
    public static void main(String[] args) {
        System.out.println(RandomUtil.getStr(2));
    }
    
}
