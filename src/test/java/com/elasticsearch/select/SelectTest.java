package com.elasticsearch.select;

import com.alibaba.fastjson.JSONObject;
import com.elasticsearch.common.enums.EnumEsAggs;
import com.elasticsearch.common.enums.EnumFilter;
import com.elasticsearch.common.enums.EnumSort;
import com.elasticsearch.common.vo.DataSource;
import com.elasticsearch.common.vo.Page;
import com.elasticsearch.vo.Person;
import java.util.List;

/**
 * Created by memory_fu on 2020/9/11.
 */
public class SelectTest {
    
    public static DataSource dataSource = new DataSource("172.16.1.119", "elastic", "123456");
    
    public static void main(String[] args) {
//        tempTest();
         test1();
//        test3();
//        test4();
//        test5();
//        test6();
    }
    
    public static void tempTest() {
        Select<Person> select = Select.from(Person.class, dataSource);
        
        select.addConditionAggs(EnumEsAggs.GROUPBY, "age", null)
            .addConditionAggs(EnumEsAggs.HAVING, null, 1)
            .addConditionAggs(EnumEsAggs.SIZE, null, 5)
            // 不传LIMIT和SORT参数时,默认不返回组内结果数据,仅返回分组数据
            .addConditionAggs(EnumEsAggs.LIMIT, null, 3)
            .addConditionAggs(EnumEsAggs.SORT, "age", EnumSort.ASC)
            .addConditionAggsNest(EnumEsAggs.GROUPBYDATE, "date_time", "second")
            .addConditionAggsNest(EnumEsAggs.HAVING, null, 1)
            .addConditionAggsNest(EnumEsAggs.SIZE, null, 5)
            // 不传LIMIT和SORT参数时,默认不返回组内结果数据,仅返回分组数据
            .addConditionAggsNest(EnumEsAggs.LIMIT, null, 3)
            .addConditionAggsNest(EnumEsAggs.SORT, "age", EnumSort.ASC);
        
        System.out.println(select.executeAggs());
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
        select.addCondition("age", EnumFilter.RANGE, 0, 100)//范围查询
            .addCondition("_index", EnumFilter.TERMS, "person_index")//in查询
            .addCondition("_type", EnumFilter.TERM, "data")//等于查询
            .addCondition("_type", EnumFilter.NQ, "data1")//不等于查询
            .addCondition("age", EnumFilter.NOT_EMPTY)//不为空查询
            .addCondition("id", EnumFilter.EMPTY)//为空查询
            .addCondition("age", EnumFilter.GT, -1)//大于查询
            .addCondition("age", EnumFilter.LT, 101)//小于查询
            .addCondition("age", EnumFilter.GTE, 0)//大于等于查询
            .addCondition("age", EnumFilter.LTE, 100)//小于等于查询
            .addCondition("hobby", EnumFilter.LIKE, "*88*")//模糊查询
            .addCondition("hobby", EnumFilter.NO_LIKE, "*40");//no like查询
//        log.info("====Select.from time is {}", System.currentTimeMillis() - start);
    
        long t2 = System.currentTimeMillis();
        
        // and or类型查询 eg: a=1 and (age=15 or age=16)
        select.addConditionShould("age", EnumFilter.TERM, 15);
        select.addConditionShould("age", EnumFilter.TERM, 16);
        select.addConditionShould("hobby", EnumFilter.NO_LIKE, "*40");
        
        select.addShould();// (相当于添加or条件)
        
        select.addCondition("age", EnumFilter.TERM, 1111);
        
        select.addSort("age", EnumSort.ASC); // 排序
        select.addSource("name", "age"); // 限制返回数据字段内容,减少网络传输数据,提高性能
    
        Page page = new Page(20,1); // 分页查询参数设置
        select.setPage(page);
        
//        log.info("====select.addCondition time is {}", System.currentTimeMillis() - t2);
        
        long t3 = System.currentTimeMillis();
        List<Person> execute = select.execute();
//        log.info("====select.execute time is {}", System.currentTimeMillis() - t3);
        
        System.out.println(JSONObject.toJSONString(execute));
    }
    
    /**
     * 分组查询(按字段分组)
     */
    public static void test2() {
        Select<Person> select = Select.from(Person.class, dataSource);
        
        select.addSource("age", "hobby");// 添加返回数据字段
        
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
        
        select.addSource("age", "hobby");// 添加返回数据字段
        
        select.addCondition("age", EnumFilter.NOT_EMPTY); // 过滤条件
        select.addConditionAggs(EnumEsAggs.GROUPBYDATE, "date_time", "day") // 按天分组(只能date类型字段使用)
//         不传LIMIT和SORT参数时,默认不返回组内结果数据,仅返回分组数据
            .addConditionAggs(EnumEsAggs.LIMIT, null, 3)
            .addConditionAggs(EnumEsAggs.SORT, "age", EnumSort.ASC);
        String s = select.executeAggs();
        System.out.println(s);
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
                select.clear(); // 必须调用此方法清空scrollId
                break;
            }
        }
        System.out.println(count);
    }
    
    /**
     * 嵌套分组查询(先根据时间分组,然后很据字段分组)
     */
    public static void test5() {
        Select<Person> select = Select.from(Person.class, dataSource);
        
        select.addConditionAggs(EnumEsAggs.GROUPBYDATE, "date_time", "second")
            .addConditionAggsNest(EnumEsAggs.GROUPBY, "age,hobby", null)
            .addConditionAggsNest(EnumEsAggs.HAVING, null, 1)
            .addConditionAggsNest(EnumEsAggs.SIZE, null, 3)
            // 不传LIMIT和SORT参数时,默认不返回组内结果数据,仅返回分组数据
            .addConditionAggsNest(EnumEsAggs.LIMIT, null, 1)
            .addConditionAggsNest(EnumEsAggs.SORT, "age", EnumSort.ASC);
        
        String s = select.executeAggs();
        System.out.println(s);
    }
    
    /**
     * 嵌套分组查询(先根据字段分组,然后很据时间分组)
     */
    public static void test6() {
        Select<Person> select = Select.from(Person.class, dataSource);
        
        select.addConditionAggs(EnumEsAggs.GROUPBY, "age,hobby", null)
            .addConditionAggsNest(EnumEsAggs.GROUPBYDATE, "date_time", "second");
        
        String s = select.executeAggs();
        
        System.out.println(s);
    }
}