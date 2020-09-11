package com.elasticsearch.common.vo;

import com.elasticsearch.common.enums.EnumEsAggs;
import lombok.Data;

/**
 * es 分组条件
 * Created by memory_fu on 2020/8/29.
 */
@Data
public class ConditionAggs {
    
    private EnumEsAggs esAggsEnum;
    private String fieldName;
    private Object value;
    
    public ConditionAggs() {
    }
    
    public ConditionAggs(EnumEsAggs esAggsEnum, String fieldName, Object value) {
        this.esAggsEnum = esAggsEnum;
        this.fieldName = fieldName;
        this.value = value;
    }
}
