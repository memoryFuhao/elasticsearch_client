package com.elasticsearch.select.aggs;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.elasticsearch.common.enums.EnumEsAggs;
import com.elasticsearch.common.enums.EnumFilter;
import com.elasticsearch.common.util.HttpClientUtil;
import com.elasticsearch.common.vo.ConditionAggs;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * es分组查询工具类
 * Created by memory_fu on 2019/6/11.
 */
@Slf4j
public class EsAggsQueryUtil {
    
    private static final String GROUPBY_FIELD_STR = "groupByField";
    private static final String GROUPBY_DATE_STR = "groupByDate";
    private static final String SPLIT = ",";
    
    /**
     * es分组查询通用方法
     */
    public String aggsQuery(List<ConditionAggs> conditionAggsList, JSONObject boolVo, String index,
        String esHost, String esPort, Map<String, String> headerMap) {
        
        EsAggsQueryUtil esQueryUtil = new EsAggsQueryUtil();
        String result = esQueryUtil.createResponse("0", "Success").toJSONString();
        try {
            JSONObject aggsVo = esQueryUtil.createAggsVo();
            for (int i = 0; i < conditionAggsList.size(); i++) {
                ConditionAggs conditionAggs = conditionAggsList.get(i);
                String opt = conditionAggs.getEsAggsEnum().getOpt();
                
                if (EnumEsAggs.GROUPBY.getOpt().equalsIgnoreCase(opt)
                    || EnumEsAggs.HAVING.getOpt().equalsIgnoreCase(opt)
                    || EnumEsAggs.LIMIT.getOpt().equalsIgnoreCase(opt)
                    || EnumEsAggs.SORT.getOpt().equalsIgnoreCase(opt)
                    || EnumEsAggs.SIZE.getOpt().equalsIgnoreCase(opt)) {
                    
                    esQueryUtil.createAggsQueryByGroupField(conditionAggs, aggsVo);
                }
                
                if (EnumEsAggs.GROUPBYDATE.toString().equalsIgnoreCase(opt)) {
                    esQueryUtil.createAggsQueryByGroupDate(conditionAggs, aggsVo);
                }
            }
            
            String requestUrl = esQueryUtil.createRequestUrl(index, esHost, esPort);
            result = esQueryUtil.getResponseStr(boolVo, aggsVo, requestUrl, headerMap);
            
        } catch (Exception e) {
            log.error("======groupBy query exception:", e);
            JSONObject failResult = esQueryUtil.createResponse("-1", "Fail");
            return failResult.toJSONString();
        }
        
        return result;
    }
    
    /**
     * 获取响应字符串
     *
     * @param boolVo 过滤条件
     * @param aggsVo 分组条件
     * @param requestUrl 请求url
     */
    public String getResponseStr(JSONObject boolVo, JSONObject aggsVo, String requestUrl,
        Map<String, String> headerMap) throws Exception {
        
        String result;
        // 组装请求参数
        JSONObject jsonObject = createRequestBody(boolVo, aggsVo);
        log.info("======request body:" + JSONObject.toJSONString(jsonObject));
        String httpResopnseStr = HttpClientUtil
            .doPost(requestUrl, jsonObject.toJSONString(), headerMap);
        
        // 解析请求结果
        JSONObject resultStr = getResult(JSONObject.parseObject(httpResopnseStr));
        result = JSONObject
            .toJSONString(resultStr, SerializerFeature.DisableCircularReferenceDetect);
        return result;
    }
    
    /**
     * 请求body
     *
     * @param boolVo 过滤条件
     * @param aggsVo 分组条件
     */
    public JSONObject createRequestBody(JSONObject boolVo, JSONObject aggsVo) {
        boolVo.put("aggs", aggsVo);
        return boolVo;
    }
    
    /**
     * 创建请求url
     *
     * @param index 索引名
     * @param esHost es ip
     * @param esPort es port
     */
    public String createRequestUrl(String index, String esHost, String esPort) {
        StringBuilder url = new StringBuilder(
            "http://" + esHost + ":" + esPort + "/" + index + "/_search");
        return url.toString();
    }
    
    public JSONObject createAggsVo() {
        return new JSONObject();
    }
    
    public JSONObject createAggsQueryByGroupDate(ConditionAggs conditionAggs, JSONObject aggsVo) {
        
        createDefaultAggs(aggsVo, GROUPBY_DATE_STR);
        
        String field = conditionAggs.getFieldName();
        Object value = conditionAggs.getValue();
        
        JSONObject groupByDate = aggsVo.getJSONObject(GROUPBY_DATE_STR);
        JSONObject jsonObjectSub = new JSONObject();
        
        jsonObjectSub.put(EnumEsAggs.FIELD.getOpt(), field);
        jsonObjectSub.put("interval", value);
        
        groupByDate.put("date_histogram", jsonObjectSub);
        
        aggsVo.put(GROUPBY_DATE_STR, groupByDate);
        return groupByDate;
    }
    
    public JSONObject createDefaultAggs(JSONObject aggsVo, String groupByStr) {
        
        JSONObject jsonObject = aggsVo.getJSONObject(groupByStr);
        
        if (null != jsonObject) {
            return aggsVo;
        }
        
        JSONObject parentJsonObject = new JSONObject();
        JSONObject aggsJsonObject = new JSONObject();

//        JSONObject topJsonObject = new JSONObject();
//        JSONObject topHitsJsonObject = new JSONObject();
//        topHitsJsonObject.put("size", 1);
//        topJsonObject.put(EsAggsEnum.TOP_HITS.getOpt(), topHitsJsonObject);
//        aggsJsonObject.put("top", topJsonObject);
        
        parentJsonObject.put("aggs", aggsJsonObject);
        
        aggsVo.put(groupByStr, parentJsonObject);
        return parentJsonObject;
    }
    
    public JSONObject createAggsQueryByGroupField(ConditionAggs conditionAggs, JSONObject aggsVo) {
        
        createDefaultAggs(aggsVo, GROUPBY_FIELD_STR);
        
//        String fieldStr = EnumEsAggs.FIELD.getOpt();
        String type = conditionAggs.getEsAggsEnum().toString();
        Object value = conditionAggs.getValue();
        String field = conditionAggs.getFieldName();
        JSONObject jsonObjectParent = new JSONObject();
        
        JSONObject jsonGroupByField = aggsVo.getJSONObject(GROUPBY_FIELD_STR);
        JSONObject aggsSub = jsonGroupByField.getJSONObject("aggs");
        JSONObject terms = jsonGroupByField.getJSONObject(EnumFilter.TERMS.getOpt());
        
        if (EnumEsAggs.GROUPBY.toString().equalsIgnoreCase(type) ||
            EnumEsAggs.SIZE.toString().equalsIgnoreCase(type)) {
            
            if (null != terms) {
                jsonObjectParent = terms;
            }
            
            if (EnumEsAggs.GROUPBY.toString().equalsIgnoreCase(type)) {
                String[] fields = field.split(SPLIT);
                String scriptStr = "";
                for (String tempField : fields) {
                    scriptStr += "doc." + tempField + "+''+";
                }
                scriptStr = scriptStr.substring(0, scriptStr.length() - 4);
//                jsonObjectParent.put(fieldStr, field);
                jsonObjectParent.put(EnumEsAggs.SCRIPT.getOpt(),scriptStr);
                jsonGroupByField.put(EnumFilter.TERMS.getOpt(), jsonObjectParent);
            }
            
            if (EnumEsAggs.SIZE.toString().equalsIgnoreCase(type)) {
                jsonObjectParent.put(EnumEsAggs.SIZE.getOpt(), value);
                jsonGroupByField.put(EnumFilter.TERMS.getOpt(), jsonObjectParent);
            }
        } else if (EnumEsAggs.HAVING.toString().equalsIgnoreCase(type)) {
            JSONObject jsonObjectBucketSelector = new JSONObject();
            JSONObject jsonObjectBucketsPath = new JSONObject();
            jsonObjectBucketsPath.put("havingCount", "_count");
            JSONObject jsonObjectScript = new JSONObject();
            jsonObjectScript.put("source", "params.havingCount >= " + value);
            
            jsonObjectBucketSelector.put("buckets_path", jsonObjectBucketsPath);
            jsonObjectBucketSelector.put("script", jsonObjectScript);
            
            jsonObjectParent.put("bucket_selector", jsonObjectBucketSelector);
            
            aggsSub.put("having", jsonObjectParent);
            
        } else if (EnumEsAggs.LIMIT.toString().equalsIgnoreCase(type)
            || EnumEsAggs.SORT.toString().equalsIgnoreCase(type)) {
            
            JSONObject jsonTop = aggsSub.getJSONObject("top");
            if (null == jsonTop) {
                jsonTop = new JSONObject();
            }
            
            JSONObject jsonTopHits = jsonTop.getJSONObject(EnumEsAggs.TOP_HITS.getOpt());
            if (null == jsonTopHits) {
                jsonTopHits = new JSONObject();
            }
            
            if (EnumEsAggs.LIMIT.toString().equalsIgnoreCase(type)) {
                jsonTopHits.put("size", value);
            } else {
                JSONObject jsonSort = new JSONObject();
                jsonSort.put(String.valueOf(field), String.valueOf(value));
                jsonTopHits.put("sort", jsonSort);
            }
            
            jsonTop.put(EnumEsAggs.TOP_HITS.getOpt(), jsonTopHits);
            aggsSub.put("top", jsonTop);
        }
        
        return jsonObjectParent;
    }
    
    public JSONArray analysisAggs(JSONObject jsonResultParent, JSONObject jsonAggs,
        String groupByStr) {
        
        JSONArray jsonResultSub = new JSONArray();
        JSONObject aggregations = jsonAggs.getJSONObject("aggregations");
        if (null == aggregations) {
            return jsonResultSub;
        }
        
        JSONObject groupByField = aggregations.getJSONObject(groupByStr);
        if (null == groupByField) {
            return jsonResultSub;
        }
        
        JSONArray buckets = groupByField.getJSONArray("buckets");
        if (null == buckets) {
            return jsonResultSub;
        }
        
        for (int i = 0; i < buckets.size(); i++) {
            JSONObject jsonResultBucket = new JSONObject();
            
            JSONObject bucketsJSONObject = buckets.getJSONObject(i);
            String groupByKey = bucketsJSONObject.getString("key");
            Long groupByCount = bucketsJSONObject.getLong("doc_count");
            JSONObject top = bucketsJSONObject.getJSONObject("top");
            
            if (null != top) {
                JSONObject parentHits = top.getJSONObject("hits");
                JSONArray subHits = parentHits.getJSONArray("hits");
                JSONArray jsonResultHits = new JSONArray();
                for (int j = 0; j < subHits.size(); j++) {
                    JSONObject source = subHits.getJSONObject(j).getJSONObject("_source");
                    jsonResultHits.add(source);
                }
                jsonResultBucket.put("hits", jsonResultHits);
            }
            
            jsonResultBucket.put("groupByKey", groupByKey);
            jsonResultBucket.put("groupByCount", groupByCount);
            jsonResultSub.add(jsonResultBucket);
        }
        
        jsonResultParent.put(groupByStr + "TotalCount", buckets.size());
        jsonResultParent.put(groupByStr + "List", jsonResultSub);
        
        return jsonResultSub;
        
    }
    
    public JSONObject getResult(JSONObject jsonObject) {
        JSONObject jsonResultParent = createResponse("0", "Success");
        
        try {
            
            analysisAggs(jsonResultParent, jsonObject, GROUPBY_FIELD_STR);
            analysisAggs(jsonResultParent, jsonObject, GROUPBY_DATE_STR);
            
        } catch (Exception e) {
            log.error("======analysis groupBy query result exception:", e);
            jsonResultParent = createResponse("-1", "Fail");
        }
        
        return jsonResultParent;
    }
    
    public JSONObject createResponse(String ret, String desc) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("ret", ret);
        jsonObject.put("desc", desc);
        return jsonObject;
    }
    
}
