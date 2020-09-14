package com.elasticsearch.update;

import static com.elasticsearch.delete.DeleteTest.dataSource;
import static org.junit.Assert.*;

import com.elasticsearch.common.enums.EnumFilter;
import com.elasticsearch.common.vo.DataSource;
import com.elasticsearch.vo.Person;

/**
 * Created by memory_fu on 2020/9/14.
 */
public class UpdateTest {
    
    public static DataSource dataSource = new DataSource("172.16.1.119", "elastic", "123456");
    
    public static void main(String[] args) {
        test1();
    }
    
    /**
     * 根据查询修改数据
     */
    public static void test1() {
        Update<Person> update = Update.from(Person.class, dataSource);
        update.addCondition("id", EnumFilter.TERM, "222");
        String age = update.updateByQuery("age", "12345");
        System.out.println(age);
    }
}