package com.elasticsearch.common.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;

/**
 * @author memory_fu
 */
public class DateUtil {
    
    public static final String DEFAULT_DATE_FORMAT = "yyyyMMdd";
    
    /**
     * 获取日期格式化字符串
     * <p>
     * 参数示例: 1358220029879L yyyy/MM/dd HH:mm:ss
     *
     * @param time 日期值
     * @param outRegex 输出模式
     */
    public static String getDateStr(long time, String outRegex) {
        Date date = new Date(time);
        
        SimpleDateFormat dateFormatterTwo = new SimpleDateFormat(outRegex);
        return dateFormatterTwo.format(date);
    }
    
    public static String getDateStr(int day) {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, -day);
        String yesterday = new SimpleDateFormat("yyyyMMdd").format(cal.getTime());
        return yesterday;
    }
    
    /**
     * 获取时间的差值
     *
     * @param stopTime 结束时间(精确到s)
     * @param startTime 开始时间(精确到s)
     * @param inRegex 输入模式
     * @return 时间差值(单位:s)
     */
    public static long getDateDifference(String stopTime, String startTime, String inRegex)
        throws ParseException {
        SimpleDateFormat dateFormatter = new SimpleDateFormat(inRegex);
        Date stopDate = dateFormatter.parse(stopTime);
        Date startDate = dateFormatter.parse(startTime);
        return (stopDate.getTime() - startDate.getTime()) / 1000;
    }
    
    /**
     * 获取当前日期格式化字符串
     * <p>
     * 参数示例: 2012/02/13 17:40:31 yyyy/MM/dd HH:mm:ss yyyyMMddHHmmss
     *
     * @param dateStr 日期字符串
     * @param inRegex 输入模式
     * @param outRegex 输出模式
     */
    public static String getDateStr(String dateStr, String inRegex, String outRegex)
        throws ParseException {
        SimpleDateFormat dateFormatter = new SimpleDateFormat(inRegex);
        Date date = dateFormatter.parse(dateStr);
        
        SimpleDateFormat dateFormatterTwo = new SimpleDateFormat(outRegex);
        return dateFormatterTwo.format(date);
    }
    
    public static String getDateStr(String timeMillons, String outRegex) throws ParseException {
        Long dateMillions = Math.round(Double.valueOf(timeMillons) * 1000);
        Date date = new Date(dateMillions);
        SimpleDateFormat dateFormatterTwo = new SimpleDateFormat(outRegex);
        return dateFormatterTwo.format(date);
    }
    
    /**
     * 获取当前日期格式化字符串
     * <p>
     * 参数示例: yyyyMMddHHmmss
     */
    public static String getCurrentDateStr(String regex) {
        return new SimpleDateFormat(regex).format(new Date());
    }
    
    /**
     * 获取某年某月的下一个月
     *
     * @param yearAndMonth yyyyMM
     */
    public static String getNextMonth(String yearAndMonth) throws ParseException {
        SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyyMM");
        Date date = dateFormatter.parse(yearAndMonth);
        
        Calendar currentCalendar = Calendar.getInstance();
        currentCalendar.setTime(date);
        currentCalendar.add(Calendar.MONTH, 1);
        
        return dateFormatter.format(currentCalendar.getTime());
    }
    
    /**
     * 二月
     */
    private static int MONTH_FEB = 2;
    
    private static int LEAP_YEAR_FOUR = 4;
    private static int LEAP_YEAR_ONE_HUNRED = 100;
    private static int LEAP_YEAR_FOUR_HUNDRED = 400;
    
    /**
     * 获取月的最后一天
     *
     * @param month 月份从1开始计数
     * @return 月的天, 从1开始计数
     */
    public static int getLastDayOfMonth(int year, int month) {
        int[] days = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
        
        boolean isLeapYear = MONTH_FEB == month && 0 == (year % LEAP_YEAR_FOUR)
            && (0 != (year % LEAP_YEAR_ONE_HUNRED) || 0 == (year % LEAP_YEAR_FOUR_HUNDRED));
        if (isLeapYear) {
            days[1] = 29;
        }
        return (days[month - 1]);
    }
    
    /**
     * 得到某年某周的第一天
     */
    public static Date getFirstDayOfWeek(int year, int week) {
        Calendar c = new GregorianCalendar();
        c.set(Calendar.YEAR, year);
        c.set(Calendar.MONTH, Calendar.JANUARY);
        c.set(Calendar.DATE, 1);
        
        Calendar cal = (GregorianCalendar) c.clone();
        cal.add(Calendar.DATE, week * 7);
        
        return getFirstDayOfWeek(cal.getTime());
    }
    
    /**
     * 得到某年某周的最后一天
     */
    public static Date getLastDayOfWeek(int year, int week) {
        Calendar c = new GregorianCalendar();
        c.set(Calendar.YEAR, year);
        c.set(Calendar.MONTH, Calendar.JANUARY);
        c.set(Calendar.DATE, 1);
        
        Calendar cal = (GregorianCalendar) c.clone();
        cal.add(Calendar.DATE, week * 7);
        
        return getLastDayOfWeek(cal.getTime());
    }
    
    /**
     * 取得指定日期所在周的第一天
     */
    public static Date getFirstDayOfWeek(Date date) {
        Calendar c = new GregorianCalendar();
        c.setFirstDayOfWeek(Calendar.MONDAY);
        c.setTime(date);
        
        // Monday
        c.set(Calendar.DAY_OF_WEEK, c.getFirstDayOfWeek());
        return c.getTime();
    }
    
    /**
     * 取得指定日期所在周的最后一天
     */
    public static Date getLastDayOfWeek(Date date) {
        Calendar c = new GregorianCalendar();
        c.setFirstDayOfWeek(Calendar.MONDAY);
        c.setTime(date);
        
        // Sunday
        c.set(Calendar.DAY_OF_WEEK, c.getFirstDayOfWeek() + 6);
        return c.getTime();
    }
    
    /**
     * 获取指定日期前的几天
     *
     * @param yearAndMonthAndDay 当前天数
     * @param inRegex 输入格式
     * @param days 多少天
     * @param outRegex 输出格式
     */
    public static String getDateBeforeDay(String yearAndMonthAndDay, String inRegex, int days,
        String outRegex)
        throws ParseException {
        SimpleDateFormat inFormatter = new SimpleDateFormat(inRegex);
        SimpleDateFormat outFormatter = new SimpleDateFormat(outRegex);
        Date date = inFormatter.parse(yearAndMonthAndDay);
        
        Calendar currentCalendar = Calendar.getInstance();
        currentCalendar.setTime(date);
        currentCalendar.add(Calendar.DAY_OF_YEAR, days);
        
        return outFormatter.format(currentCalendar.getTime());
    }
    
    /**
     * 获取某个月的天数
     */
    public static int getDaysOfMonth(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.getActualMaximum(Calendar.DAY_OF_MONTH);
    }
    
    /**
     * 格式化字符串日期
     *
     * @param dateStr 字符串格式的日期
     * @param pattern 日期格式
     * @return Date 日期
     */
    public static Date parse(String dateStr, String pattern) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        return sdf.parse(dateStr);
    }
    
    /**
     * <p>
     * Description 将date换化为yyyy-MM-dd格式的字符串
     * </p>
     * <p>
     * Copyright Copyright(c)2007
     * </p>
     *
     * @create time: 2007-3-2 下午02:39:30
     * @version 1.0
     * @modified records:
     */
    public static String formatToDate(Date date) {
        SimpleDateFormat formater = new SimpleDateFormat(DEFAULT_DATE_FORMAT);
        return formater.format(date);
    }
    
    public static String lastDay(String date, int size) throws ParseException {
        Date d = parse(date, DEFAULT_DATE_FORMAT);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(d);
        calendar.add(Calendar.DATE, size);
        d = calendar.getTime();
        return formatToDate(d);
    }
    
    /**
     * 判断日期是否为当月最后一天 是返回true 否返回false
     *
     * @param dateStr 日期字符串yyyyMMdd
     */
    public static boolean isLastDay(String dateStr) throws ParseException {
        boolean result = false;
        if (StringUtils.isBlank(dateStr)) {
            return result;
        }
        SimpleDateFormat sdf = new SimpleDateFormat(DEFAULT_DATE_FORMAT);
        Date date = DateUtils.addDays(sdf.parse(dateStr), 1);
        String date1Str = sdf.format(date);
        String dateMonth = dateStr.substring(4, 6);
        String date1Month = date1Str.substring(4, 6);
        if (!dateMonth.equals(date1Month)) {
            result = true;
        }
        return result;
    }
    
    /**
     * 对日期字段串进行月份增加或减少
     *
     * @param dateStr yyyyMMdd
     * @param amount 为正增加月份，为负减少月份
     */
    public static String addMonths(String dateStr, int amount) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(DEFAULT_DATE_FORMAT);
        Date date = sdf.parse(dateStr);
        Date addMonths = DateUtils.addMonths(date, amount);
        String result = sdf.format(addMonths);
        return result;
    }
    
    /**
     * 对日期字段串进行月份增加或减少
     *
     * @param dateStr yyyyMMdd
     * @param amount 为正增加月份，为负减少月份
     * @param DEFAULT_DATE_FORMAT 格式化日期字符串格式
     */
    public static String addMonths(String dateStr, int amount, String DEFAULT_DATE_FORMAT)
        throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(DEFAULT_DATE_FORMAT);
        Date date = sdf.parse(dateStr);
        Date addMonths = DateUtils.addMonths(date, amount);
        String result = sdf.format(addMonths);
        return result;
    }
    
    /**
     * 对日期字段串进行天数增加或减少
     *
     * @param dateStr yyyyMMdd
     * @param amount 为正增加天数，为负减少天数
     */
    public static String addDays(String dateStr, int amount) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(DEFAULT_DATE_FORMAT);
        Date date = sdf.parse(dateStr);
        Date addDays = DateUtils.addDays(date, amount);
        String result = sdf.format(addDays);
        return result;
    }
    
    /**
     * 根据时间字符串获取时间戳
     * @param dateStr
     * @param ddfStr
     * @return
     */
    public static long getTimeLong(String dateStr,String ddfStr) throws ParseException {
        String ddf = ddfStr;
        if(StringUtils.isEmpty(ddf)){
            ddf = DEFAULT_DATE_FORMAT;
        }
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(ddf);
    
        long time = simpleDateFormat.parse(dateStr).getTime();
        return time;
    }
    
    /**
     * 根据时间字符串获取时间戳
     * @param dateStr
     * @return
     * @throws ParseException
     */
    public static long getTimeLong(String dateStr) throws ParseException {
        return getTimeLong(dateStr,null);
    }
    
    
}
