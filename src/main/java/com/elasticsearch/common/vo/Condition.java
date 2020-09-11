package com.elasticsearch.common.vo;

import com.elasticsearch.common.enums.EnumFilter;
import java.util.List;
import lombok.Data;

/**
 * es 查询条件
 * Created by memory_fu on 2020/6/18.
 */
@Data
public class Condition {
    
    private String fieldName;
    private EnumFilter enumFilter;
    private List<Object> values;
    
    public Condition() {}
    
    public Condition(String fieldName, EnumFilter enumFilter, List<Object> values) {
        this.fieldName = fieldName;
        this.enumFilter = enumFilter;
        this.values = values;
    }
}
