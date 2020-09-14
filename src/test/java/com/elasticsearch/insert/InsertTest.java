package com.elasticsearch.insert;

import com.elasticsearch.common.vo.DataSource;
import com.elasticsearch.vo.Person;

/**
 * Created by memory_fu on 2020/9/11.
 */
public class InsertTest {
    public static DataSource dataSource = new DataSource("172.16.1.119", "elastic", "123456");
    
    public static void main(String[] args) {
        test1();
    }
    
    /**
     * 数据插入
     */
    public static void test1(){
        
        Insert<Person> from = Insert.from(Person.class, dataSource);
        for (int i = 0; i < 20000; i++) {
            Person person = new Person();
//            person.setId(i + "");
            person.setHobby("兴趣爱好:" + i);
            person.setAge(i);
            person.setName("name:" + i);
            from.add(person);
        }
        int execute = from.execute();
        System.out.println(execute);
    }
    
}