package com.elasticsearch.delete;

import com.alibaba.fastjson.JSONObject;
import com.elasticsearch.common.Operation;
import com.elasticsearch.common.enums.EnumEsKeyword;
import com.elasticsearch.common.util.HttpClientUtil;
import com.elasticsearch.common.vo.DataSource;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * Created by memory_fu on 2020/8/31.
 */
@Data
@Slf4j
public class Delete<T> extends Operation {
    
    public static <T> Delete<T> from(Class<T> tClass, DataSource dataSource) {
        Delete delete = new Delete();
        delete.init(tClass, dataSource);
        return delete;
    }
    
    /**
     * 执行删除请求(根据条件)
     *
     * @return 返回删除条数，是否执行成功信息
     */
    public String deleteByQuery() {
        JSONObject jsonObject = analysisParaJson();
        jsonObject.remove(EnumEsKeyword.FROM.getOpt());
        jsonObject.remove(EnumEsKeyword.SIZE.getOpt());
        String postBody = jsonObject.toJSONString();
        String url = getUrl("_delete_by_query?pretty");
        log.info("====【DeleteByQuery execute】 url is:{} \n postBody is: {}", url, postBody);
        
        JSONObject result = createResponse("0", "Success");
        String res = null;
        try {
            res = HttpClientUtil.doPost(url, postBody, this.getHeaderMap());
            JSONObject resJsonObject = JSONObject.parseObject(res);
            long deleted = resJsonObject.getLongValue("deleted");
            result.put("deleted", deleted);
        } catch (Exception e) {
            result = createResponse("-1", "Fail");
            result.put("errMessage", res);
            log.error("====DeleteByQuery execute exception,url :{} body:{}", url, postBody, e);
        }
        return result.toJSONString();
    }
    
    /**
     * 支持删除请求(根据文档Id)
     *
     * @param documentId 文档Id
     */
    public String deleteById(String documentId) {
        String url = getUrl("_doc/" + documentId);
        log.info("====【DeleteById execute】 url is:{}", url);
        
        JSONObject result = createResponse("0", "Success");
        String res = null;
        try {
            res = HttpClientUtil.doDelete(url, this.getHeaderMap());
            JSONObject resJsonObject = JSONObject.parseObject(res);
            
            String deletedStr = resJsonObject.getString("result");
            int deleted = 0;
            if ("deleted".equalsIgnoreCase(deletedStr)) {
                deleted = 1;// 删除成功
            }
            result.put("deleted", deleted);
        } catch (Exception e) {
            result = createResponse("-1", "Fail");
            result.put("errMessage", res);
            log.error("====DeleteById execute exception,url :{}", url, e);
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
