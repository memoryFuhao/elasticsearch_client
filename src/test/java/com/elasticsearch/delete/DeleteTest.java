package com.elasticsearch.delete;

import static com.elasticsearch.insert.InsertTest.dataSource;
import static org.junit.Assert.*;

import com.elasticsearch.common.enums.EnumFilter;
import com.elasticsearch.common.vo.DataSource;
import com.elasticsearch.vo.Person;

/**
 * Created by memory_fu on 2020/9/14.
 */
public class DeleteTest {
    
    public static DataSource dataSource = new DataSource("172.16.1.119", "elastic", "123456");
    
    public static void main(String[] args) {
        test1();
        test2();
    }
    
    /**
     * 根据查询删除数据
     */
    public static void test1() {
        Delete<Person> delete = Delete.from(Person.class, dataSource);
        delete.addCondition("id", EnumFilter.TERM, "0");
        String s = delete.deleteByQuery();
        System.out.println(s);
    }
    
    /**
     * 根据id删除数据
     */
    public static void test2() {
        Delete<Person> delete = Delete.from(Person.class, dataSource);
        String res = delete.deleteById("文档id");
        System.out.println(res);
    }
}