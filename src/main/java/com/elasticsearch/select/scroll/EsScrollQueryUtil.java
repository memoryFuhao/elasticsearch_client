package com.elasticsearch.select.scroll;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.elasticsearch.common.enums.EnumEsKeyword;
import com.elasticsearch.common.util.HttpClientUtil;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

/**
 * @author memory_fu
 */
@Slf4j
@Data
public class EsScrollQueryUtil {
    
    private String scrollId = null;
    
    /**
     * 全量遍历es数据
     *
     * @param indexName 索引名称
     * @param ip 安装es地址
     * @param scrollTime scrollId保存时间
     * @param size 每次查询es数据条数
     */
    public List<JSONObject> queryData(String indexName, String ip, String port, String scrollTime,
        Map<String, String> headerMap, JSONObject queryObj, int size)
        throws Exception {
        
        List<JSONObject> result = Lists.newArrayList();
        if (StringUtils.isEmpty(this.scrollId)) {
            result = getScrollId(indexName, ip, port, scrollTime, headerMap, queryObj, size);
        } else {
            result = getScrollData(indexName, ip, port, scrollTime, headerMap, size);
        }
        return result;
    }
    
    /**
     * 全量遍历完后，清除scrollId记录
     */
    public void clear() {
        this.scrollId = null;
    }
    
    private List<JSONObject> getScrollId(String indexName, String ip, String port,
        String scrollTime, Map<String, String> headerMap, JSONObject queryObj, int size)
        throws Exception {
        // 获取srcollId和数据
        String url = getUrl(indexName, ip, port, scrollTime);
        queryObj.put(EnumEsKeyword.SIZE.getOpt(), size);
        queryObj.remove(EnumEsKeyword.FROM.getOpt());
        
        log.info("====scroll query url is:{} \n queryStr is:{}", url, queryObj.toJSONString());
        
        String queryStr = queryObj.toJSONString();
        String responseStr = HttpClientUtil.doPost(url, queryStr, headerMap);
        List<JSONObject> analysisData = analysisData(responseStr);
        
        return analysisData;
    }
    
    private List<JSONObject> getScrollData(String indexName, String ip, String port,
        String scrollTime, Map<String, String> headerMap, int size) throws Exception {
        
        String url = getUrl(indexName, ip, port, scrollTime);
        
        JSONObject jsonObject = new JSONObject();
        jsonObject.put(EnumEsKeyword.SCROLL.getOpt(), scrollTime);
        jsonObject.put(EnumEsKeyword.SCROLL_ID.getOpt(), this.scrollId);
        String queryStr = jsonObject.toJSONString();
        log.info("====scroll query url is:{} \n queryStr is:{}", url, queryStr);
        
        String responseStr = HttpClientUtil.doPost(url, queryStr, headerMap);
        List<JSONObject> analysisData = analysisData(responseStr);
        return analysisData;
    }
    
    private List<JSONObject> analysisData(String responseStr) {
        List<JSONObject> list = new ArrayList<>();
        
        JSONObject jsonObject = JSONObject.parseObject(responseStr);
        if (null == jsonObject) {
            return list;
        }
        
        this.scrollId = jsonObject.getString(EnumEsKeyword._SCROLL_ID.getOpt());
        
        JSONObject parentHits = jsonObject.getJSONObject(EnumEsKeyword.HITS.getOpt());
        if (null == parentHits) {
            return list;
        }
        JSONArray subHits = parentHits.getJSONArray(EnumEsKeyword.HITS.getOpt());
        if (null == subHits) {
            return list;
        }
        
        for (int i = 0; i < subHits.size(); i++) {
            JSONObject object = subHits.getJSONObject(i);
            String id = object.getString("_id");
            JSONObject source = object.getJSONObject(EnumEsKeyword.SOURCE.getOpt());
            source.put("id", id);
            list.add(source);
        }
        
        return list;
    }
    
    /**
     * 根据是否为第一次查询
     */
    private String getUrl(String indexName, String ip, String port, String scrollTime) {
        if (StringUtils.isEmpty(this.scrollId)) {
            return new String("http://" + ip + ":" + port + "/" + indexName + "/_search?scroll="
                + scrollTime);
        }
        return new String("http://" + ip + ":" + port + "/_search/scroll");
    }
    
}
