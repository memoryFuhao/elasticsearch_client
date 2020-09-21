package com.elasticsearch.insert;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.elasticsearch.common.Operation;
import com.elasticsearch.common.util.HttpClientUtil;
import com.elasticsearch.common.util.RandomUtil;
import com.elasticsearch.common.vo.DataSource;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * Created by memory_fu on 2020/7/8.
 */
@Data
@Slf4j
public class Insert<T> extends Operation {
    
    private StringBuilder postBody = new StringBuilder();
    
    public static <T> Insert<T> from(Class<T> tClass, DataSource dataSource) {
        Insert insert = new Insert();
        insert.init(tClass, dataSource);
        return insert;
    }
    
    /**
     * 添加入库对象
     *
     * @param vo 数据对象
     */
    public void add(T vo) {
        String s = JSON.toJSONString(vo);
        JSONObject jsonVo = JSON.parseObject(s);
        
        Map<String, String> fieldAnMap = getFieldAnMap();
        JSONObject jsonObject = new JSONObject();
        Object id = null;
        for (Entry<String, Object> entry : jsonVo.entrySet()) {
            String key = entry.getKey();
            String anVal = fieldAnMap.get(key);
            Object val = jsonVo.get(key);
            if ("_id".equalsIgnoreCase(anVal)) {
                id = val;
                continue;
            }
            if (StringUtils.isNotEmpty(anVal)) {
                jsonObject.put(anVal, val);
            }
        }
        
        JSONObject jsonIndex = new JSONObject();
        jsonIndex.put("_index", this.getIndexNameStr());
        jsonIndex.put("_type", "data");
        if (null != id) {
            jsonIndex.put("_id", id);
        }
        JSONObject jsonIndexParent = new JSONObject();
        jsonIndexParent.put("index", jsonIndex);
        
        this.postBody =
            postBody.append(jsonIndexParent.toJSONString()).append("\n")
                .append(jsonObject.toJSONString()).append("\n");
    }
    
    /**
     * 批量插入数据
     *
     * @param list 入库对象集合
     */
    public void add(List<T> list) {
        if (CollectionUtils.isEmpty(list)) {
            return;
        }
        for (T vo : list) {
            add(vo);
        }
    }
    
    /**
     * 执行插入请求
     *
     * @return -1:插入失败  其他:插入成功数据条数
     */
    public int execute() {
        int count = -1;
        String response = null;
        String url = getUrl("_bulk?pretty");
        try {
            response = HttpClientUtil.doPost(url, this.postBody.toString(), this.getHeaderMap());
            JSONObject jsonObject = JSON.parseObject(response);
            String errors = jsonObject.getString("errors");
            if (StringUtils.isNotEmpty(errors) && !Boolean.valueOf(errors)) {
                count = jsonObject.getJSONArray("items").size();
            } else {
                log.error("====Insert execute error,response:{}", response);
            }
        } catch (Exception e) {
            log.error("====Insert execute exception,url:{} response:{}", url, response, e);
        } finally {
            this.postBody.setLength(0);
        }
        return count;
    }
    
    @Override
    public String getUrl(String type) {
        DataSource dataSource = this.getDataSource();
        int anInt = RandomUtil.getInt(dataSource.getIps().length);
        return dataSource.getProtocol() + "://" + dataSource.getIps()[anInt] + ":" + dataSource
            .getPort() + "/" + type;
    }
    
}
