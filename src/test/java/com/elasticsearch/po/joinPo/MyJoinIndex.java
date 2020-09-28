package com.elasticsearch.po.joinPo;

import com.elasticsearch.common.annotation.Index;
import com.elasticsearch.common.annotation.Propertie;
import lombok.Data;

/**
 * Created by memory_fu on 2020/8/31.
 */
@Data
@Index(IndexName = "my_join_index")
public class MyJoinIndex {
    
    @Propertie(PropertieName = "name")
    String name;
    
    @Propertie(PropertieName = "join_field")
    Object joinField;
    
}
