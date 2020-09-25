package com.elasticsearch.insert;

import com.elasticsearch.common.vo.DataSource;
import com.elasticsearch.po.Person;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by memory_fu on 2020/9/11.
 */
public class InsertTest {
    
    public static DataSource dataSource = new DataSource("172.16.1.119", "elastic", "123456");
    
    public static void main(String[] args) {
        test1();
//        test2();
    }
    
    /**
     * 单线程数据插入(插入对象为com.elasticsearch.vo.Person)   50w数据插入耗时22000ms
     */
    public static void test1() {
        long startTime = System.currentTimeMillis();
        Insert<Person> from = Insert.from(Person.class, dataSource);
        for (int j = 0; j < 50; j++) {
            for (int i = 0; i < 10000; i++) {
                Person person = new Person();
//            person.setId(i + "");
                person.setHobby("兴趣爱好:" + i);
                person.setAge(i);
                person.setName("name:" + i);
                person.setDateTime(new Date().getTime());
                from.add(person);
            }
            int execute = from.execute();
            System.out.println(execute);
        }
        
        System.out.println("插入时间:" + (System.currentTimeMillis() - startTime) + "ms");
    }
    
    /**
     * 多线程数据插入(插入对象为com.elasticsearch.vo.Person)   50w数据插入耗时11800ms
     */
    public static void test2() {
        long startTime = System.currentTimeMillis();
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
            .setNameFormat("Insert es data").build();
        ThreadPoolExecutor savePool = new ThreadPoolExecutor(4, 4, 0L,
            TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(10000), threadFactory);
        
        List<Future<?>> objects = Lists.newArrayList();
        for (int j = 0; j < 50; j++) {
            Future<?> submit = savePool.submit(() -> insertData());
            objects.add(submit);
        }
        
        savePool.shutdown();
        
        for (Future<?> future : objects) {
            try {
                Object o = future.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        
        System.out.println("插入时间:" + (System.currentTimeMillis() - startTime) + "ms");
    }
    
    public static void insertData() {
        Insert<Person> from = Insert.from(Person.class, dataSource);
        for (int i = 0; i < 10000; i++) {
            Person person = new Person();
            person.setHobby("兴趣爱好:" + i);
            person.setAge(i);
            person.setName("name:" + i);
            person.setDateTime(new Date().getTime());
            from.add(person);
        }
        from.execute();
    }
    
}