package com.elasticsearch.common.vo;

import lombok.Data;

/**
 * 分页对象
 *
 *
 * Created by memory_fu on 2020/7/1.
 */
@Data
public class Page {
    
    public static final int MAX_DATA_COUNT = 10000; // es分页临界值
    public static final String SCROLL = "5m";       // es深度分页保存时间minute
    public static final int SCROLL_SIZE = 3000;     // es深度分页size
    private int requestCount;                       // es深度分页时http请求次数
    private int discardBatchCount;                  // es深度分页时丢弃的数据批次,每个批次大小为SCROLL_SIZE对应值
    
    private int pageSize = 20;
    private int pageNum = 1;
    private int from;
    private int to;
    
    
    public Page() {
    
    }
    
    public Page(int pageSize, int pageNum) {
        this.pageSize = pageSize;
        this.pageNum = pageNum;
        this.from = (pageNum - 1) * pageSize;
        this.to = pageNum * pageSize;
        setPagePreBatch();
    }
    
    /**
     * 深度分页时，计算出 es深度分页时http请求次数 & es深度分页时丢弃的数据批次
     */
    private void setPagePreBatch() {
        int res = this.getPageSize() * this.getPageNum() / this.SCROLL_SIZE;
        int i = this.getPageSize() * this.getPageNum() % this.SCROLL_SIZE;
        
        if (i == 0) {
            this.setRequestCount(res);
            this.setDiscardBatchCount(res - 1);
        } else {
            this.setRequestCount(res + 1);
            this.setDiscardBatchCount(res);
        }
        
    }
    
    
}
