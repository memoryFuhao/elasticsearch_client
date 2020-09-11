package com.elasticsearch.common.enums;

/**
 * Created by memory_fu on 2020/8/26.
 */
public enum EnumSort {
    
    ASC("asc"),
    DESC("desc");
    
    private String opt;
    
    private EnumSort(String opt) {
        this.opt = opt;
    }
    
    public String getOpt() {
        return this.opt;
    }
    
}
