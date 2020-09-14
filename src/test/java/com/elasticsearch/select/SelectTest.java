package com.elasticsearch.select;

import com.alibaba.fastjson.JSONObject;
import com.elasticsearch.common.enums.EnumEsAggs;
import com.elasticsearch.common.enums.EnumFilter;
import com.elasticsearch.common.enums.EnumSort;
import com.elasticsearch.common.vo.DataSource;
import com.elasticsearch.vo.Person;
import java.util.List;

/**
 * Created by memory_fu on 2020/9/11.
 */
public class SelectTest {
    
    public static DataSource dataSource = new DataSource("172.16.1.119", "elastic", "123456");
    
    public static void main(String[] args) {
        tempTest();
//        test1();
//        test2();
//        test3();
    }
    
    public static void tempTest() {
        Select<Person> from = Select.from(Person.class, dataSource);
        
        from.addCondition("age", EnumFilter.RANGE, 10, 100)
            .addConditionShould("age", EnumFilter.TERMS, 50, 55, 66)
            .addConditionShould("hobby", EnumFilter.NO_LIKE, "*6");
        
        List<Person> execute = from.execute();
        System.out.println(JSONObject.toJSONString(execute));
    }
    
    /**
     * 条件组合查询(支持and、or、isNull、isNotNull、in、like、no_like、大于、大于等于、小于、小于等于、不等于、范围)
     * 支持 a=1 and (b=2 or b=3) 查询
     * 支持 ( a=1 and (b=2 or b=3)) or (a=2 and (b=1 or b =2)) 查询
     *
     * 请自行进行查询组合,组合完成的json串请参看日志信息。
     */
    public static void test1() {
        long start = System.currentTimeMillis();
        Select<Person> select = Select.from(Person.class, dataSource);
//        log.info("====Select.from time is {}", System.currentTimeMillis() - start);
        
        long t2 = System.currentTimeMillis();
        select.addCondition("age", EnumFilter.RANGE, 0, 100)
            .addCondition("hobby", EnumFilter.LIKE, "*88*")
            .addCondition("_type", EnumFilter.TERM, "data")
            .addCondition("_index", EnumFilter.TERMS, "person_index")
            .addCondition("age", EnumFilter.NOT_EMPTY)
            .addCondition("id", EnumFilter.EMPTY)
            .addCondition("_type", EnumFilter.NQ, "data1")
            .addCondition("age", EnumFilter.GT, -1)
            .addCondition("age", EnumFilter.LT, 101)
            .addCondition("age", EnumFilter.GTE, 0)
            .addCondition("age", EnumFilter.LTE, 100)
            .addCondition("hobby", EnumFilter.NO_LIKE, "*40");
        
        // and or类型查询 eg: a=1 and (age=15 or age=16)
        select.addConditionShould("age", EnumFilter.TERM, 15);
        select.addConditionShould("age", EnumFilter.TERM, 16);
        select.addConditionShould("hobby", EnumFilter.NO_LIKE, "*40");
        
        select.addShould();// (相当于添加or条件)
        
        select.addCondition("age", EnumFilter.TERM, 1111);
        
        select.addSort("age", EnumSort.ASC); // 排序
        
        select.addSource("name", "age"); // 限制返回数据字段内容,减少网络传输数据,提高性能

//        log.info("====select.addCondition time is {}", System.currentTimeMillis() - t2);
        
        long t3 = System.currentTimeMillis();
        List<Person> execute = select.execute();
//        log.info("====select.execute time is {}", System.currentTimeMillis() - t3);
        
        System.out.println(execute.size());
    }
    
    /**
     * 分组查询(按字段分组)
     */
    public static void test2() {
        Select<Person> select = Select.from(Person.class, dataSource);
        
        select.addConditionAggs(EnumEsAggs.GROUPBY, "age,hobby", null) // 根据age、hobby字段对数据进行分组
            .addConditionAggs(EnumEsAggs.HAVING, null, 1)
            .addConditionAggs(EnumEsAggs.SIZE, null, 5)
        // 不传LIMIT和SORT参数时,默认不返回组内结果数据,仅返回分组数据
//            .addConditionAggs(EnumEsAggs.LIMIT, null, 3)
//            .addConditionAggs(EnumEsAggs.SORT, "age", EnumSort.ASC);
        ;
        String s = select.executeAggs();
        
        System.out.println(s);
    }
    
    /**
     * 分组查询(按时间分组)
     */
    public static void test3() {
        Select<Person> select = Select.from(Person.class, dataSource);
        
        select.addCondition("age", EnumFilter.NOT_EMPTY); // 过滤条件
        select.addConditionAggs(EnumEsAggs.GROUPBYDATE, null, "day"); // 按天分组(只能丢date类型字段使用)
        
        List<Person> execute = select.execute();
        System.out.println(JSONObject.toJSONString(execute));
    }
    
    /**
     * scroll遍历(支持条件过滤后获取全部数据 || 遍历索引全部数据)
     */
    public static void test4() {
        Select<Person> select = Select.from(Person.class, dataSource);
        int pageSize = 5000;
        int count = 0;
        while (true) {
            List<Person> scroll = select.scroll(pageSize);
            count += scroll.size();
            if (scroll.size() < pageSize) {
                break;
            }
        }
        System.out.println(count);
    }
}