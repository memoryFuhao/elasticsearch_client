package com.elasticsearch.select.aggs;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.elasticsearch.common.enums.EnumEsAggs;
import com.elasticsearch.common.enums.EnumEsKeyword;
import com.elasticsearch.common.enums.EnumFilter;
import com.elasticsearch.common.util.HttpClientUtil;
import com.elasticsearch.common.vo.ConditionAggs;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * es分组查询工具类
 * Created by memory_fu on 2019/6/11.
 */
@Slf4j
public class EsAggsQueryUtil {
    
    private static final String SPLIT = ",";
    
    /**
     * es分组查询通用方法
     *
     * @param conditionAggsList 分组条件集合
     * @param conditionAggsNestList 嵌套分组条件集合
     * @param boolVo 过滤条件json串
     * @param index 索引名称
     * @param esHost es ip地址
     * @param esPort es端口
     * @param headerMap header集合
     * @param sourceList 返回字段集合
     */
    public String aggsQuery(List<ConditionAggs> conditionAggsList,
        List<ConditionAggs> conditionAggsNestList, JSONObject boolVo, String index,
        String esHost, String esPort, Map<String, String> headerMap, List<String> sourceList) {
        
        EsAggsQueryUtil esQueryUtil = new EsAggsQueryUtil();
        String result = StringUtils.EMPTY;
        try {
            JSONObject aggsVo = createAggsVo(conditionAggsList, sourceList);
            if (CollectionUtils.isNotEmpty(conditionAggsNestList)) {
                JSONObject aggsVoNest = createAggsVo(conditionAggsNestList, sourceList);
                putConditionAggsNest(aggsVo, aggsVoNest, EnumEsKeyword.GROUPBY_FIELD);
                putConditionAggsNest(aggsVo, aggsVoNest, EnumEsKeyword.GROUPBY_DATE);
            }
            
            String requestUrl = esQueryUtil.createRequestUrl(index, esHost, esPort);
            result = esQueryUtil.getResponseStr(boolVo, aggsVo, requestUrl, headerMap);
            
        } catch (Exception e) {
            log.error("======groupBy query exception:", e);
            result = createResponse("-1", "Fail").toJSONString();
        }
        
        return result;
    }
    
    /**
     * 组装分组嵌套json串
     *
     * @param aggsVo 分组父条件
     * @param aggsVoNest 分组子条件
     * @param enumEsKeyword 分组类型
     */
    private void putConditionAggsNest(JSONObject aggsVo, JSONObject aggsVoNest,
        EnumEsKeyword enumEsKeyword) {
        JSONObject jsonObject = aggsVo.getJSONObject(enumEsKeyword.getOpt());
        if (null != jsonObject) {
            jsonObject.put(EnumEsKeyword.AGGS.getOpt(), aggsVoNest);
        }
    }
    
    private JSONObject createAggsVo(List<ConditionAggs> conditionAggsList,
        List<String> sourceList) {
        JSONObject aggsVo = new JSONObject();
        boolean groupByDateFlag = checkGroupType(conditionAggsList, EnumEsAggs.GROUPBYDATE);
        boolean groupByFlag = checkGroupType(conditionAggsList, EnumEsAggs.GROUPBY);
        for (int i = 0; i < conditionAggsList.size(); i++) {
            ConditionAggs conditionAggs = conditionAggsList.get(i);
            String opt = conditionAggs.getEsAggsEnum().getOpt();
            
            if (groupByFlag && (EnumEsAggs.GROUPBY.getOpt().equalsIgnoreCase(opt)
                || EnumEsAggs.HAVING.getOpt().equalsIgnoreCase(opt)
                || EnumEsAggs.LIMIT.getOpt().equalsIgnoreCase(opt)
                || EnumEsAggs.SORT.getOpt().equalsIgnoreCase(opt)
                || EnumEsAggs.SIZE.getOpt().equalsIgnoreCase(opt))) {
                createAggsQueryByGroupField(conditionAggs, aggsVo);
            }
            
            if (EnumEsAggs.GROUPBYDATE.toString().equalsIgnoreCase(opt) && groupByDateFlag) {
                createAggsQueryByGroupDate(conditionAggs, aggsVo);
            }
        }
        addSource(aggsVo, sourceList);
        return aggsVo;
    }
    
    /**
     * 检查分组条件是否包含指定分组类型
     *
     * @param conditionAggsList 分组条件集合
     * @param enumEsAggs 分组类型
     * @return 包含:true  不包含:false
     */
    private boolean checkGroupType(List<ConditionAggs> conditionAggsList, EnumEsAggs enumEsAggs) {
        for (ConditionAggs conditionAggs : conditionAggsList) {
            if (enumEsAggs.equals(conditionAggs.getEsAggsEnum())) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * 指定返回数据的字段列表
     *
     * @param aggsVo 分组查询json串
     * @param sourceList 返回字段列表
     */
    private void addSource(JSONObject aggsVo, List<String> sourceList) {
        checkAndAddSource(aggsVo, sourceList, EnumEsKeyword.GROUPBY_DATE.getOpt());
        checkAndAddSource(aggsVo, sourceList, EnumEsKeyword.GROUPBY_FIELD.getOpt());
    }
    
    /**
     * 判断分组后是否需要返回数据, 需要返回数据时则指定返回数据的字段列表
     *
     * @param aggsVo 分组查询json串
     * @param sourceList 返回字段列表
     * @param groupByType 分组类型
     */
    private void checkAndAddSource(JSONObject aggsVo, List<String> sourceList, String groupByType) {
        if (CollectionUtils.isEmpty(sourceList)) {
            return;
        }
        JSONObject jsonObject = aggsVo.getJSONObject(groupByType);
        if (null == jsonObject) {
            return;
        }
        JSONObject aggs = jsonObject.getJSONObject(EnumEsKeyword.AGGS.getOpt());
        if (null == aggs) {
            return;
        }
        JSONObject top = aggs.getJSONObject(EnumEsKeyword.TOP.getOpt());
        if (null == top) {
            return;
        }
        JSONObject topHits = top.getJSONObject(EnumEsKeyword.TOP_HITS.getOpt());
        if (null != topHits) {
            topHits.put(EnumEsKeyword.SOURCE.getOpt(), sourceList);
        }
    }
    
    /**
     * 获取响应字符串
     *
     * @param boolVo 过滤条件
     * @param aggsVo 分组条件
     * @param requestUrl 请求url
     */
    private String getResponseStr(JSONObject boolVo, JSONObject aggsVo, String requestUrl,
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
    private JSONObject createRequestBody(JSONObject boolVo, JSONObject aggsVo) {
        boolVo.put(EnumEsKeyword.AGGS.getOpt(), aggsVo);
        return boolVo;
    }
    
    /**
     * 创建请求url
     *
     * @param index 索引名
     * @param esHost es ip
     * @param esPort es port
     */
    private String createRequestUrl(String index, String esHost, String esPort) {
        StringBuilder url = new StringBuilder(
            "http://" + esHost + ":" + esPort + "/" + index + "/_search");
        return url.toString();
    }
    
    private void createAggsQueryByGroupDate(ConditionAggs conditionAggs, JSONObject aggsVo) {
        
        createDefaultAggs(aggsVo, EnumEsKeyword.GROUPBY_DATE.getOpt());
        
        String field = conditionAggs.getFieldName();
        Object value = conditionAggs.getValue();
        
        JSONObject groupByDate = aggsVo.getJSONObject(EnumEsKeyword.GROUPBY_DATE.getOpt());
        JSONObject jsonObjectSub = new JSONObject();
        
        jsonObjectSub.put(EnumEsKeyword.FIELD.getOpt(), field);
        jsonObjectSub.put("interval", value);
        jsonObjectSub.put("time_zone", "+08:00");
        groupByDate.put("date_histogram", jsonObjectSub);
        
        aggsVo.put(EnumEsKeyword.GROUPBY_DATE.getOpt(), groupByDate);
    }
    
    private void createAggsQueryByGroupField(ConditionAggs conditionAggs, JSONObject aggsVo) {
        
        createDefaultAggs(aggsVo, EnumEsKeyword.GROUPBY_FIELD.getOpt());
        
        String type = conditionAggs.getEsAggsEnum().toString();
        Object value = conditionAggs.getValue();
        String field = conditionAggs.getFieldName();
        JSONObject jsonObjectParent = new JSONObject();
        
        JSONObject jsonGroupByField = aggsVo.getJSONObject(EnumEsKeyword.GROUPBY_FIELD.getOpt());
        JSONObject aggsSub = jsonGroupByField.getJSONObject(EnumEsKeyword.AGGS.getOpt());
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
                jsonObjectParent.put(EnumEsKeyword.SCRIPT.getOpt(), scriptStr);
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
            jsonObjectBucketSelector.put(EnumEsKeyword.SCRIPT.getOpt(), jsonObjectScript);
            
            jsonObjectParent.put("bucket_selector", jsonObjectBucketSelector);
            
            aggsSub.put(EnumEsAggs.HAVING.getOpt(), jsonObjectParent);
            
        } else if (EnumEsAggs.LIMIT.toString().equalsIgnoreCase(type)
            || EnumEsAggs.SORT.toString().equalsIgnoreCase(type)) {
            
            JSONObject jsonTop = aggsSub.getJSONObject(EnumEsKeyword.TOP.getOpt());
            if (null == jsonTop) {
                jsonTop = new JSONObject();
            }
            
            JSONObject jsonTopHits = jsonTop.getJSONObject(EnumEsKeyword.TOP_HITS.getOpt());
            if (null == jsonTopHits) {
                jsonTopHits = new JSONObject();
            }
            
            if (EnumEsAggs.LIMIT.toString().equalsIgnoreCase(type)) {
                jsonTopHits.put(EnumEsKeyword.SIZE.getOpt(), value);
            } else {
                JSONObject jsonSort = new JSONObject();
                jsonSort.put(String.valueOf(field), String.valueOf(value));
                jsonTopHits.put(EnumEsKeyword.SORT.getOpt(), jsonSort);
            }
            
            jsonTop.put(EnumEsKeyword.TOP_HITS.getOpt(), jsonTopHits);
            aggsSub.put(EnumEsKeyword.TOP.getOpt(), jsonTop);
        }
    }
    
    private void createDefaultAggs(JSONObject aggsVo, String groupByStr) {
        
        JSONObject jsonObject = aggsVo.getJSONObject(groupByStr);
        
        if (null != jsonObject) {
            return;
        }
        
        JSONObject parentJsonObject = new JSONObject();
        JSONObject aggsJsonObject = new JSONObject();
        
        parentJsonObject.put(EnumEsKeyword.AGGS.getOpt(), aggsJsonObject);
        
        aggsVo.put(groupByStr, parentJsonObject);
    }
    
    private JSONObject analysisAggs(JSONObject jsonResultParent, JSONObject aggregations,
        String groupByStr) {
        
        JSONArray jsonResultSub = new JSONArray();
        JSONObject groupByField = aggregations.getJSONObject(groupByStr);
        if (null == groupByField) {
            return jsonResultParent;
        }
        JSONArray buckets = groupByField.getJSONArray("buckets");
        if (null == buckets) {
            return jsonResultParent;
        }
        for (int i = 0; i < buckets.size(); i++) {
            JSONObject jsonResultBucket = new JSONObject();
            
            JSONObject bucketsJSONObject = buckets.getJSONObject(i);
            String groupByKey = bucketsJSONObject.getString("key");
            Long groupByCount = bucketsJSONObject.getLong("doc_count");
            JSONObject top = bucketsJSONObject.getJSONObject(EnumEsKeyword.TOP.getOpt());
            
            //递归解析分组结果
            analysisAggs(jsonResultBucket, bucketsJSONObject, EnumEsKeyword.GROUPBY_FIELD.getOpt());
            analysisAggs(jsonResultBucket, bucketsJSONObject, EnumEsKeyword.GROUPBY_DATE.getOpt());
            
            if (null != top) {
                JSONObject parentHits = top.getJSONObject(EnumEsKeyword.HITS.getOpt());
                JSONArray subHits = parentHits.getJSONArray(EnumEsKeyword.HITS.getOpt());
                JSONArray jsonResultHits = new JSONArray();
                for (int j = 0; j < subHits.size(); j++) {
                    JSONObject source = subHits.getJSONObject(j)
                        .getJSONObject(EnumEsKeyword.SOURCE.getOpt());
                    jsonResultHits.add(source);
                }
                jsonResultBucket.put(EnumEsKeyword.HITS.getOpt(), jsonResultHits);
            }
            
            jsonResultBucket.put("groupByKey", groupByKey);
            jsonResultBucket.put("groupByCount", groupByCount);
            jsonResultSub.add(jsonResultBucket);
        }
        
        jsonResultParent.put(groupByStr + "TotalCount", buckets.size());
        jsonResultParent.put(groupByStr + "List", jsonResultSub);
        
        return jsonResultParent;
    }
    
    private JSONObject getResult(JSONObject jsonObject) {
        JSONObject jsonResultParent = createResponse("0", "Success");
        
        try {
            JSONObject aggregations = jsonObject.getJSONObject(EnumEsKeyword.AGGREGATIONS.getOpt());
            if (null != aggregations) {
                analysisAggs(jsonResultParent, aggregations, EnumEsKeyword.GROUPBY_FIELD.getOpt());
                analysisAggs(jsonResultParent, aggregations, EnumEsKeyword.GROUPBY_DATE.getOpt());
            }
        } catch (Exception e) {
            log.error("======analysis groupBy query result exception:", e);
            jsonResultParent = createResponse("-1", "Fail");
        }
        
        return jsonResultParent;
    }
    
    private JSONObject createResponse(String ret, String desc) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("ret", ret);
        jsonObject.put("desc", desc);
        return jsonObject;
    }
    
}
