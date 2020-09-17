package com.elasticsearch.vo;

import com.elasticsearch.common.annotation.Index;
import com.elasticsearch.common.annotation.Propertie;
import com.elasticsearch.common.enums.EnumEsAggs;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import lombok.Data;

/**
 * Created by memory_fu on 2020/8/31.
 */
@Data
@Index(IndexName = "person_index")
public class Person extends Parent {
    
    @Propertie(PropertieName = "hobby")
    String hobby;
    
    @Propertie(PropertieName = "date_time")
    long dateTime;
    
}
