package com.elasticsearch.common.vo;

import lombok.Data;

/**
 * es 链接信息
 * Created by memory_fu on 2020/6/19.
 */
@Data
public class DataSource {
    
    /**
     * 协议类型
     */
    private String protocol = "http";
    
    /**
     * ip地址集合
     */
    private String[] ips;
    
    /**
     * 端口
     */
    private String port = "9200";
    
    /**
     * es用户名
     */
    private String userName;
    
    /**
     * es用户密码
     */
    private String password;
    
    public DataSource() {
    }
    
    public DataSource(String ips) {
        this.ips = ips.split(",");
    }
    
    public DataSource(String ips, String port) {
        this.ips = ips.split(",");
        this.port = port;
    }
    
    public DataSource(String ips,String userName, String password) {
        this.ips = ips.split(",");
        this.userName = userName;
        this.password = password;
    }
    
    public DataSource(String protocol, String ips, String port, String userName,
        String password) {
        this.protocol = protocol;
        this.ips = ips.split(",");
        this.port = port;
        this.userName = userName;
        this.password = password;
    }
}
