package com.elasticsearch.common.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Created by memory_fu on 2020/6/19.
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface Index {
    
    /**
     * 索引名称
     */
    String IndexName();
    
}
