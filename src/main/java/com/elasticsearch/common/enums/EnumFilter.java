package com.elasticsearch.common.enums;

/**
 * Created by memory_fu on 2020/6/18.
 */
public enum EnumFilter {
    
    TERM("term"),// 等于
    TERMS("terms"),// in
    RANGE("range"),// 范围
    GTE("gte"),// 大于等于
    LTE("lte"),// 小于等于
    GT("gt"),// 大于
    LT("lt"),// 小于
    NQ("must_not"),// 不等于
    LIKE("wildcard"),// like (用于must查询)
    NO_LIKE(""),// no like (用于must_not查询)
    NOT_EMPTY("exists"),// 存在
    EMPTY("not_exists");// 不存在
    
    private String opt;
    
    private EnumFilter(String opt) {
        this.opt = opt;
    }
    
    public String getOpt() {
        return this.opt;
    }
    
}
