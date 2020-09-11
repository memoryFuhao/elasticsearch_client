package com.elasticsearch.update;

import com.alibaba.fastjson.JSONObject;
import com.elasticsearch.common.Operation;
import com.elasticsearch.common.util.HttpClientUtil;
import com.elasticsearch.common.vo.DataSource;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * Created by memory_fu on 2020/8/31.
 */
@Data
@Slf4j
public class Update<T> extends Operation {
    
    public static <T> Update<T> from(Class<T> tClass, DataSource dataSource) {
        Update update = new Update();
        update.init(tClass, dataSource);
        return update;
    }
    
    /**
     * 执行修改请求
     *
     * @param fieldName 字段名称
     * @param fieldVal 字段内容
     */
    public String updateByQuery(String fieldName, Object fieldVal) {
        
        JSONObject scriptJson = new JSONObject();
        scriptJson.put("source", "ctx._source." + fieldName + "=\"" + fieldVal + "\"");
        
        JSONObject jsonObject = analysisParaJson();
        jsonObject.remove("from");
        jsonObject.remove("size");
        jsonObject.put("script", scriptJson);
        
        String url = getUrl("_update_by_query");
        String postBody = jsonObject.toJSONString();
        log.info("====【UpdateByQuery execute】 url is:{} \n postBody is: {}", url, postBody);
        JSONObject result = createResponse("0", "Success");
        String res = null;
        try {
            res = HttpClientUtil.doPost(url, postBody, this.getHeaderMap());
            JSONObject resJsonObject = JSONObject.parseObject(res);
            long updated = resJsonObject.getLongValue("updated");
            result.put("updated", updated);
        } catch (Exception e) {
            result = createResponse("-1", "Fail");
            result.put("errMessage", res);
            log.error("====UpdateByQuery execute exception,url :{} body:{}", url, postBody, e);
        }
        return result.toJSONString();
    }
    
    private JSONObject createResponse(String ret, String desc) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("ret", ret);
        jsonObject.put("desc", desc);
        return jsonObject;
    }
    
}
