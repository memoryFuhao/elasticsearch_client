package com.elasticsearch.common;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.elasticsearch.common.annotation.Index;
import com.elasticsearch.common.annotation.Propertie;
import com.elasticsearch.common.enums.EnumEsKeyword;
import com.elasticsearch.common.enums.EnumFilter;
import com.elasticsearch.common.enums.EnumSort;
import com.elasticsearch.common.util.RandomUtil;
import com.elasticsearch.common.vo.Condition;
import com.elasticsearch.common.vo.DataSource;
import com.elasticsearch.common.vo.Page;
import com.elasticsearch.common.vo.Sort;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;

/**
 * Created by memory_fu on 2020/6/18.
 */
@Data
@Slf4j
public abstract class Operation<T> {
    
    protected Page page = new Page();// 分页对象
    protected DataSource dataSource;// 数据源
    protected AtomicInteger counter = new AtomicInteger(1);// should条件累加标识位
    protected Map<Integer, List<Condition>> conditionsMap = Maps.newConcurrentMap();// 查询条件must集合
    protected Map<Integer, List<Condition>> conditionsShouldMap = Maps
        .newConcurrentMap();// 查询条件should集合
    protected List<Sort> sortsList = Lists.newArrayList();// 排序条件集合
    protected Map<String, String> headerMap = Maps.newHashMap();// header集合
    protected Map<String, String> anFieldMap = Maps.newConcurrentMap();// <注解,字段>映射关系
    protected Map<String, String> fieldAnMap = Maps.newConcurrentMap();// <字段,注解>映射关系
    protected List<String> sourceList = Lists.newArrayList();// 所需操作es字段集合
    protected String indexNameStr;// 索引名称
    protected String auth;// 认证信息
    protected T vo;
    
    /**
     * 添加过滤条件
     *
     * @param fieldName 字段名称(必填)
     * @param enumFilter 过来枚举类型(必填)
     * @param fieldValue 字段值
     * @return this
     */
    public Operation<T> addCondition(String fieldName, EnumFilter enumFilter,
        Object... fieldValue) {
        assembleCondition(conditionsMap, fieldName, enumFilter, fieldValue);
        return this;
    }
    
    /**
     * 添加should条件
     *
     * @param fieldName 字段名称(必填)
     * @param enumFilter 过来枚举类型(必填)
     * @param fieldValue 字段值
     */
    public Operation<T> addConditionShould(String fieldName, EnumFilter enumFilter,
        Object... fieldValue) {
        assembleCondition(conditionsShouldMap, fieldName, enumFilter, fieldValue);
        return this;
    }
    
    /**
     * 添加排序条件
     *
     * @param fieldName 排序字段名称
     * @param enumSort 排序类型(asc、desc)
     * @return this
     */
    public Operation<T> addSort(String fieldName, EnumSort enumSort) {
        sortsList.add(new Sort(fieldName, enumSort.getOpt()));
        return this;
    }
    
    /**
     * should标识位(相当于添加or)
     *
     * @return this
     */
    public Operation<T> addShould() {
        this.counter.addAndGet(1);
        return this;
    }
    
    /**
     * 添加source条件，限制返回字段；不添加默认返回全部字段
     *
     * @param sources 字段名称集合
     * @return this
     */
    public Operation<T> addSource(String... sources) {
        this.sourceList = Lists.newArrayList(sources);
        return this;
    }
    
    /**
     * 初始化
     *
     * @param tClass 对象类类型
     * @param dataSource 数据源
     */
    protected void init(Class<T> tClass, DataSource dataSource) {
        try {
            this.vo = tClass.newInstance();
            this.dataSource = dataSource;
            this.auth = "Basic " + new String(
                Base64.getEncoder().encode(
                    (dataSource.getUserName() + ":" + dataSource.getPassword()).getBytes()));
            headerMap.put("Authorization", auth);
            this.indexNameStr = getIndexName();
            objAndAnnotationMap();
        } catch (InstantiationException e) {
            log.error("[{} 对象构建失败]", tClass, e);
        } catch (IllegalAccessException e) {
            log.error("[{} 对象构建失败]", tClass, e);
        }
    }
    
    /**
     * 拼装es请求url
     */
    protected String getUrl(String type) {
        int anInt = RandomUtil.getInt(dataSource.getIps().length);
        String url =
            dataSource.getProtocol() + "://" + dataSource.getIps()[anInt] + ":" + dataSource
                .getPort() + "/" + this.indexNameStr + "/" + type;
        
        return judgePage() ? url : (url + "?scroll=" + Page.SCROLL);
    }
    
    /**
     * 拼装Scroll url
     */
    protected String getScrollUrl() {
        int anInt = RandomUtil.getInt(dataSource.getIps().length);
        return dataSource.getProtocol() + "://" + dataSource.getIps()[anInt] + ":" + dataSource
            .getPort() + "/_search/scroll";
    }
    
    /**
     * 判断分页是否在10000条内
     *
     * @return 10000条数据内true  10000条数据外false
     */
    protected boolean judgePage() {
        return this.page.getPageNum() * this.page.getPageSize() < Page.MAX_DATA_COUNT;
    }
    
    /**
     * 解析参数,拼装成es格式json对象
     */
    protected JSONObject analysisParaJson() {
        JSONObject parJson = createParJson();
        Map<Integer, List<Condition>> conditionsMap = getConditionsMap();
        for (Map.Entry<Integer, List<Condition>> entry : conditionsMap.entrySet()) {
            JSONObject baseJson = createBaseJson(EnumEsKeyword.MUST);
            List<Condition> conditions = entry.getValue();
            for (Condition condition : conditions) {
                analysisEnumFilter(condition, baseJson, EnumEsKeyword.MUST);
            }
            
            Integer key = entry.getKey();
            List<Condition> conditionsShould = conditionsShouldMap.get(key);
            if (CollectionUtils.isNotEmpty(conditionsShould)) {
                JSONObject baseShouldJson = createBaseJson(EnumEsKeyword.SHOULD);
                for (Condition condition : conditionsShould) {
                    analysisEnumFilter(condition, baseShouldJson, EnumEsKeyword.SHOULD);
                }
                addJsonObjectForAndShould(baseJson, baseShouldJson);
            }
            
            addJsonObjectForShould(baseJson, parJson);
        }
        addPagePara(parJson);
        return parJson;
    }
    
    /**
     * 分析过滤条件
     *
     * @param condition 过滤条件
     * @param baseJson 父json串
     * @param esKeyword 类型
     */
    private void analysisEnumFilter(Condition condition, JSONObject baseJson,
        EnumEsKeyword esKeyword) {
        EnumFilter enumFilter = condition.getEnumFilter();
        
        JSONObject addMustJson = new JSONObject();
        if (EnumFilter.TERM.equals(enumFilter)) {
            addMustJson = createTermJson(condition);
        }
        if (EnumFilter.TERMS.equals(enumFilter)) {
            addMustJson = createTermsJson(condition);
        }
        if (EnumFilter.GTE.equals(enumFilter) || EnumFilter.LTE.equals(enumFilter)
            || EnumFilter.GT.equals(enumFilter) || EnumFilter.LT.equals(enumFilter)) {
            addMustJson = createCompareJson(condition);
        }
        if (EnumFilter.LIKE.equals(enumFilter)) {
            addMustJson = createLikeJson(condition);
        }
        if (EnumFilter.RANGE.equals(enumFilter)) {
            addMustJson = createRangeJson(condition);
        }
        if (EnumFilter.NOT_EMPTY.equals(enumFilter)) {
            addMustJson = createExistsJson(condition);
        }
        if (!addMustJson.isEmpty()) {
            addJsonObjectForMust(addMustJson, baseJson, esKeyword);
        }
        
        JSONObject addMustNotJson = new JSONObject();
        if (EnumFilter.NQ.equals(enumFilter)) {
            addMustNotJson = createNqJson(condition);
        }
        if (EnumFilter.NO_LIKE.equals(enumFilter)) {
            addMustNotJson = createLikeJson(condition);
        }
        if (EnumFilter.EMPTY.equals(enumFilter)) {
            addMustNotJson = createExistsJson(condition);
        }
        if (!addMustNotJson.isEmpty()) {
            addJsonObjectForMustNot(addMustNotJson, baseJson);
        }
    }
    
    /**
     * 添加分页参数
     */
    private void addPagePara(JSONObject jsonObject) {
        if (judgePage()) {
            jsonObject.put(EnumEsKeyword.FROM.getOpt(),
                page.getPageSize() * page.getPageNum() - page.getPageSize());
            jsonObject.put(EnumEsKeyword.SIZE.getOpt(), page.getPageSize());
        } else {
            jsonObject.put(EnumEsKeyword.SIZE.getOpt(), Page.SCROLL_SIZE);
        }
    }
    
    /**
     * 构建term查询json串
     */
    private JSONObject createTermJson(Condition condition) {
        JSONObject jsonObject = new JSONObject();
        JSONObject termJsonObject = new JSONObject();
        termJsonObject.put(condition.getFieldName(), condition.getValues().get(0));
        jsonObject.put(EnumFilter.TERM.getOpt(), termJsonObject);
        return jsonObject;
    }
    
    /**
     * 构建terms查询json串
     */
    private JSONObject createTermsJson(Condition condition) {
        JSONObject jsonObject = new JSONObject();
        JSONObject termsJsonObject = new JSONObject();
        termsJsonObject.put(condition.getFieldName(), condition.getValues());
        jsonObject.put(EnumFilter.TERMS.getOpt(), termsJsonObject);
        return jsonObject;
    }
    
    /**
     * 构建like查询json串
     */
    private JSONObject createLikeJson(Condition condition) {
        JSONObject jsonObject = new JSONObject();
        JSONObject wildcardJsonObject = new JSONObject();
        wildcardJsonObject.put(condition.getFieldName(), condition.getValues().get(0));
        jsonObject.put(EnumFilter.LIKE.getOpt(), wildcardJsonObject);
        return jsonObject;
    }
    
    /**
     * 构建nq查询json串(不等于查询)
     */
    private JSONObject createNqJson(Condition condition) {
        JSONObject jsonObject = new JSONObject();
        JSONObject termJsonObject = new JSONObject();
        List<Object> values = condition.getValues();
        if (values.size() == 1) {
            termJsonObject.put(condition.getFieldName(), condition.getValues().get(0));
            jsonObject.put(EnumFilter.TERM.getOpt(), termJsonObject);
        } else if (values.size() == 2) {
            JSONObject rangeJson = new JSONObject();
            rangeJson.put(EnumFilter.GTE.getOpt(), condition.getValues().get(0));
            rangeJson.put(EnumFilter.LTE.getOpt(), condition.getValues().get(1));
            termJsonObject.put(condition.getFieldName(), rangeJson);
            jsonObject.put(EnumFilter.RANGE.getOpt(), termJsonObject);
        }
        return jsonObject;
    }
    
    /**
     * 构建range查询json串
     */
    private JSONObject createRangeJson(Condition condition) {
        JSONObject jsonObject = new JSONObject();
        JSONObject rangeJsonObject = new JSONObject();
        JSONObject fieldjsonObject = new JSONObject();
        fieldjsonObject.put(EnumFilter.GTE.getOpt(), condition.getValues().get(0));
        fieldjsonObject.put(EnumFilter.LT.getOpt(), condition.getValues().get(1));
        rangeJsonObject.put(condition.getFieldName(), fieldjsonObject);
        jsonObject.put(EnumFilter.RANGE.getOpt(), rangeJsonObject);
        return jsonObject;
    }
    
    /**
     * 构建exists查询json串
     */
    private JSONObject createExistsJson(Condition condition) {
        JSONObject jsonObject = new JSONObject();
        JSONObject existsJsonObject = new JSONObject();
        existsJsonObject.put(EnumEsKeyword.FIELD.getOpt(), condition.getFieldName());
        jsonObject.put(EnumFilter.NOT_EMPTY.getOpt(), existsJsonObject);
        return jsonObject;
    }
    
    /**
     * 获取索引名称
     */
    private String getIndexName() {
        Annotation[] annotations = getVo().getClass().getAnnotations();
        String indexName = null;
        for (Annotation annotation : annotations) {
            if (annotation instanceof Index) {
                indexName = ((Index) annotation).IndexName();
            }
        }
        this.indexNameStr = indexName;
        return indexName;
    }
    
    /**
     * 对象属性和注解映射关系
     */
    private void objAndAnnotationMap() {
        Class aClass = getVo().getClass();
        for (Class i = aClass; i != Object.class; i = i.getSuperclass()) {
            Field[] declaredFields = i.getDeclaredFields();
            for (Field field : declaredFields) {
                String fieldName = field.getName();
                Annotation[] annotations = field.getAnnotations();
                for (Annotation an : annotations) {
                    if (an instanceof Propertie) {
                        String propertieName = ((Propertie) an).PropertieName();
                        anFieldMap.put(propertieName, fieldName);
                        fieldAnMap.put(fieldName, propertieName);
                    }
                }
            }
        }
    }
    
    /**
     * 构建compare查询json串
     */
    private JSONObject createCompareJson(Condition condition) {
        JSONObject jsonObject = new JSONObject();
        JSONObject rangeJsonObject = new JSONObject();
        JSONObject fieldjsonObject = new JSONObject();
        fieldjsonObject.put(condition.getEnumFilter().getOpt(), condition.getValues().get(0));
        rangeJsonObject.put(condition.getFieldName(), fieldjsonObject);
        jsonObject.put(EnumFilter.RANGE.getOpt(), rangeJsonObject);
        return jsonObject;
    }
    
    private void addJsonObjectForMust(JSONObject addJson, JSONObject parentJson,
        EnumEsKeyword esKeyword) {
        JSONObject boolJsonObject = parentJson.getJSONObject(EnumEsKeyword.BOOL.getOpt());
        JSONArray mustJsonArray = boolJsonObject.getJSONArray(esKeyword.getOpt());
        mustJsonArray.add(addJson);
    }
    
    private void addJsonObjectForMustNot(JSONObject addJson, JSONObject parentJson) {
        JSONObject boolJsonObject = parentJson.getJSONObject(EnumEsKeyword.BOOL.getOpt());
        JSONArray mustNotJsonArray = boolJsonObject.getJSONArray(EnumEsKeyword.MUST_NOT.getOpt());
        if (null == mustNotJsonArray) {
            mustNotJsonArray = new JSONArray();
        }
        mustNotJsonArray.add(addJson);
        boolJsonObject.put(EnumEsKeyword.MUST_NOT.getOpt(), mustNotJsonArray);
    }
    
    private void addJsonObjectForShould(JSONObject addJson, JSONObject parJson) {
        JSONObject queryJsonObject = parJson.getJSONObject(EnumEsKeyword.QUERY.getOpt());
        JSONObject boolJsonObject = queryJsonObject.getJSONObject(EnumEsKeyword.BOOL.getOpt());
        JSONArray mustJsonArray = boolJsonObject.getJSONArray(EnumEsKeyword.SHOULD.getOpt());
        mustJsonArray.add(addJson);
    }
    
    private void addJsonObjectForAndShould(JSONObject baseJson, JSONObject baseShouldJson) {
        JSONObject jsonObject = baseJson.getJSONObject(EnumEsKeyword.BOOL.getOpt());
        JSONArray jsonArray = jsonObject.getJSONArray(EnumEsKeyword.MUST.getOpt());
        jsonArray.add(baseShouldJson);
    }
    
    private JSONObject createBaseJson(EnumEsKeyword esKeyword) {
        JSONArray must = new JSONArray();
        JSONObject bool = new JSONObject();
        JSONObject query = new JSONObject();
        bool.put(esKeyword.getOpt(), must);
        query.put(EnumEsKeyword.BOOL.getOpt(), bool);
        return query;
    }
    
    private JSONObject createParJson() {
        JSONObject query = createQueryJson();
        JSONObject jsonObject = new JSONObject();
        jsonObject.put(EnumEsKeyword.QUERY.getOpt(), query);
        if (CollectionUtils.isNotEmpty(sourceList)) {
            jsonObject.put(EnumEsKeyword.SOURCE.getOpt(), sourceList);
        }
        if (CollectionUtils.isNotEmpty(sortsList)) {
            JSONArray sortArr = new JSONArray();
            for (Sort sort : sortsList) {
                JSONObject sortObj = new JSONObject();
                sortObj.put(sort.getFieldName(), sort.getSortType());
                sortArr.add(sortObj);
            }
            jsonObject.put(EnumEsKeyword.SORT.getOpt(), sortArr);
        }
        return jsonObject;
    }
    
    private JSONObject createQueryJson() {
        JSONArray should = new JSONArray();
        JSONObject bool = new JSONObject();
        JSONObject query = new JSONObject();
        bool.put(EnumEsKeyword.SHOULD.getOpt(), should);
        query.put(EnumEsKeyword.BOOL.getOpt(), bool);
        return query;
    }
    
    /**
     * 创建查询条件对象
     */
    private Condition createCondition(String fieldName, EnumFilter enumFilter,
        Object... fieldValue) {
        List<Object> objects = Lists.newArrayList();
        if (fieldValue.length > 0) {// 可变长参数兼容数组和List
            Object o = fieldValue[0];
            if (o instanceof List) {
                objects = (List<Object>) o;
            } else if (o instanceof Set) {
                objects = Lists.newArrayList((Set<Object>) o);
            } else {
                objects = Lists.newArrayList(fieldValue);
            }
        }
        return new Condition(fieldName, enumFilter, objects);
    }
    
    /**
     * 组装查询条件
     */
    private void assembleCondition(Map<Integer, List<Condition>> map, String fieldName,
        EnumFilter enumFilter, Object... fieldValue) {
        Condition condition = createCondition(fieldName, enumFilter, fieldValue);
        List<Condition> conditions = map.get(counter.get());
        if (null == conditions) {
            conditions = Lists.newArrayList();
        }
        conditions.add(condition);
        map.put(counter.get(), conditions);
    }
    
}
