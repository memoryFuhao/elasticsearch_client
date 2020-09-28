package com.elasticsearch.po.joinPo;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

/**
 * Created by memory_fu on 2020/9/28.
 */
@Data
public class SonObject {
    
    @JSONField(name = "name")
    String name;
    
    @JSONField(name = "parent")
    String parent;
    
}
