package com.elasticsearch.vo;

import com.elasticsearch.common.annotation.Index;
import com.elasticsearch.common.annotation.Propertie;
import lombok.Data;

/**
 * Created by memory_fu on 2020/8/31.
 */
@Data
@Index(IndexName = "parent_index")
public class Parent {
    
    @Propertie(PropertieName = "id")
    String id;
    
    @Propertie(PropertieName = "name")
    String name;
    
    @Propertie(PropertieName = "age")
    int age;
    
    public static void main(String[] args) {
        System.out.println(1);
    }
}
