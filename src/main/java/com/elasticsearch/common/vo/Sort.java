package com.elasticsearch.common.vo;

import lombok.Data;

/**
 * Created by memory_fu on 2020/8/26.
 */
@Data
public class Sort {

    private String fieldName;
    
    private String sortType;
    
    public Sort() {
    }
    
    public Sort(String fieldName, String sortType) {
        this.fieldName = fieldName;
        this.sortType = sortType;
    }
}
