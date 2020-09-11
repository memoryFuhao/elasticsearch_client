package com.elasticsearch.select;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.elasticsearch.common.Operation;
import com.elasticsearch.common.enums.EnumEsAggs;
import com.elasticsearch.common.enums.EnumEsKeyword;
import com.elasticsearch.common.util.HttpClientUtil;
import com.elasticsearch.common.util.RandomUtil;
import com.elasticsearch.common.vo.ConditionAggs;
import com.elasticsearch.common.vo.DataSource;
import com.elasticsearch.common.vo.Page;
import com.elasticsearch.select.aggs.EsAggsQueryUtil;
import com.elasticsearch.select.scroll.EsScrollQueryUtil;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

/**
 * Created by memory_fu on 2020/6/18.
 */
@Data
@Slf4j
public class Select<T> extends Operation {
    
    private EsScrollQueryUtil esScrollQueryUtil = new EsScrollQueryUtil();// 全量遍历对象
    private EsAggsQueryUtil esAggsQueryUtil = new EsAggsQueryUtil();// 分组查询对象
    private List<ConditionAggs> conditionAggsList = Lists.newArrayList();// 分组条件集合
    
    public static <T> Select<T> from(Class<T> tClass, DataSource dataSource) {
        Select select = new Select();
        select.init(tClass, dataSource);
        return select;
    }
    
    /**
     * 添加分组条件
     *
     * @param esAggsEnum 分组操作内容
     * @param fieldName 字段名称(可为null)
     * @param value 字段内容(可为null)
     * @return 返回this
     */
    public Select<T> addConditionAggs(EnumEsAggs esAggsEnum, String fieldName, Object value) {
        ConditionAggs conditionAggs = new ConditionAggs(esAggsEnum, fieldName, value);
        conditionAggsList.add(conditionAggs);
        return this;
    }
    
    /**
     * 执行分组查询
     *
     * @return 分组结果json串
     */
    
    public String executeAggs() {
        JSONObject jsonObject = analysisParaJson();
        jsonObject.put(EnumEsKeyword.SIZE.getOpt(), 0);// 分组查询不需要返回结果数据
        DataSource ds = this.getDataSource();
        int anInt = RandomUtil.getInt(ds.getIps().length);
        String result = esAggsQueryUtil
            .aggsQuery(this.conditionAggsList, jsonObject, getIndexNameStr(), ds.getIps()[anInt],
                ds.getPort(), this.getHeaderMap());
        return result;
    }
    
    /**
     * scroll方式查询数据，对大批量数据查询推荐使用
     *
     * @param size 每次返回数据条数
     * @return 数据集合
     */
    public List<T> scroll(int size) {
        List<T> lists = Lists.newArrayList();
        try {
            DataSource ds = this.getDataSource();
            JSONObject jsonObject = null;
            if (StringUtils.isEmpty(this.esScrollQueryUtil.getScrollId())) {
                jsonObject = analysisParaJson();
            }
            int anInt = RandomUtil.getInt(ds.getIps().length);
            List<JSONObject> list = esScrollQueryUtil
                .queryData(getIndexNameStr(), ds.getIps()[anInt], ds.getPort(), Page.SCROLL,
                    this.getHeaderMap(), jsonObject, size);
            for (JSONObject jsonVo : list) {
                String sourceRes = analysisSource(jsonVo.toJSONString());
                T o = (T) JSONObject.parseObject(sourceRes, this.getVo().getClass());
                lists.add(o);
            }
        } catch (Exception e) {
            log.error("====scroll Exception:", e);
        }
        return lists;
    }
    
    /**
     * 获取数据总数
     *
     * @return 数据总条数
     */
    public Long getTotalCount() {
        Long result = 0L;
        String postBody = analysisParaJson().toJSONString();
        String url = getUrl("_search");
        log.info("====getTotalCount url is:{} \n postBody is: {}", url, postBody);
        try {
            String s = HttpClientUtil.doPost(url, postBody, this.getHeaderMap());
            JSONObject jsonObject = JSONObject.parseObject(s);
            JSONObject hits = jsonObject.getJSONObject("hits");
            result = hits.getLongValue("total");
        } catch (Exception e) {
            log.error("====getTotalCount exception,url :{} body:{}", url, postBody, e);
        }
        return result;
    }
    
    /**
     * 执行查询
     *
     * @return 结果数据集合
     */
    public List<T> execute() {
        
        String postBody = analysisParaJson().toJSONString();
        String url = getUrl("_search");
        log.info("====【Select execute】 url is:{} \n postBody is: {}", url, postBody);
        List<T> lists = Lists.newArrayList();
        try {
            if (judgePage()) {
                String s = HttpClientUtil.doPost(url, postBody, this.getHeaderMap());
                JSONObject jsonObject = JSONObject.parseObject(s);
                
                JSONObject hits = jsonObject.getJSONObject("hits");
//                long total = hits.getLongValue("total");
                JSONArray hitsArray = hits.getJSONArray("hits");
                for (int i = 0; i < hitsArray.size(); i++) {
                    JSONObject object = hitsArray.getJSONObject(i);
                    String source = object.getString("_source");
                    String sourceRes = analysisSource(source);
                    T o = (T) JSONObject.parseObject(sourceRes, this.getVo().getClass());
                    lists.add(o);
                }
            } else {
                Page page = this.getPage();
                int requestCount = page.getRequestCount();
                String scrollId = null;
                int count = 0;
                for (int i = 1; i <= requestCount; i++) {
                    String s = HttpClientUtil.doPost(url, postBody, this.getHeaderMap());
                    count += Page.SCROLL_SIZE;
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    if (i == 1) {
                        scrollId = jsonObject.getString("_scroll_id");
                        JSONObject scroll_id = new JSONObject();
                        scroll_id.put("scroll_id", scrollId);
                        scroll_id.put("scroll", page.SCROLL);
                        postBody = scroll_id.toJSONString();
                        url = getScrollUrl();
                    } else {
                        if (count < page.getFrom()) {
                            continue;
                        }
                        JSONObject hits = jsonObject.getJSONObject("hits");
                        JSONArray hitsArray = hits.getJSONArray("hits");
                        int start = page.getFrom() - (i - 1) * Page.SCROLL_SIZE;
                        start = start < 0 ? 0 : start;
                        int end = start + page.getPageSize() - lists.size();
                        end = end > hitsArray.size() ? hitsArray.size() : end;
                        
                        List<Object> objects =
                            (end < start) ? hitsArray : hitsArray.subList(start, end);
                        JSONArray hitsArraySub = JSONArray
                            .parseArray(JSONObject.toJSONString(objects));
                        for (int j = 0; j < hitsArraySub.size(); j++) {
                            JSONObject object = hitsArraySub.getJSONObject(j);
                            String source = object.getString("_source");
                            String sourceRes = analysisSource(source);
                            T o = (T) JSONObject.parseObject(sourceRes, this.getVo().getClass());
                            lists.add(o);
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("====execute exception,url :{} body:{}", url, postBody, e);
        }
        return lists;
    }
    
    /**
     * 遍历完成后，清楚scollId记录
     */
    public void clear() {
        this.esScrollQueryUtil.clear();
    }
    
    /**
     * 解析json串，将对象属性和注解对应
     */
    private String analysisSource(String source) {
        JSONObject result = new JSONObject();
        JSONObject sourceJson = JSONObject.parseObject(source);
        Set<Entry<String, Object>> entries = sourceJson.entrySet();
        Map<String, String> anMap = getAnFieldMap();
        for (Entry<String, Object> entry : entries) {
            String key = entry.getKey();
            String objKey = anMap.get(key);
            if (null != objKey) {
                result.put(objKey, entry.getValue());
            }
        }
        return result.toJSONString();
    }
    
}