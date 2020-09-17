package com.elasticsearch.common.enums;

/**
 * Created by memory_fu on 2020/6/18.
 */
public enum EnumEsKeyword {
    
    QUERY("query"),
    BOOL("bool"),
    SHOULD("should"),
    MUST("must"),
    MUST_NOT("must_not"),
    FROM("from"),
    SIZE("size"),
    SOURCE("_source"),
    SORT("sort"),
    FIELD("field"),
    HITS("hits"),
    TOP("top"),
    TOP_HITS("top_hits"),
    AGGS("aggs"),
    SCRIPT("script"),
    TOTAL("total"),
    SCROLL("scroll"),
    SCROLL_ID("scroll_id"),
    _SCROLL_ID("_scroll_id"),
    VALUE("value"),
    AGGREGATIONS("aggregations"),
    GROUPBY_FIELD("groupByField"),
    GROUPBY_DATE("groupByDate");
    
    private String opt;
    
    private EnumEsKeyword(String opt) {
        this.opt = opt;
    }
    
    public String getOpt() {
        return this.opt;
    }
    
    public static void main(String[] args) {
        System.out.println(EnumEsKeyword.MUST.getOpt());
    }
}
