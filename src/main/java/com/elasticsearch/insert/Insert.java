package com.elasticsearch.insert;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.elasticsearch.common.Operation;
import com.elasticsearch.common.util.HttpClientUtil;
import com.elasticsearch.common.util.RandomUtil;
import com.elasticsearch.common.vo.DataSource;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
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
        try {
            
            String s = JSON.toJSONString(vo);
            JSONObject jsonVo = JSON.parseObject(s);

//            JSONObject jsonVo = convertJSON(vo);
            
            Map<String, String> fieldAnMap = getFieldAnMap();
            JSONObject jsonObject = new JSONObject();
            Object id = null;
            Object parentId = null;
            for (Entry<String, Object> entry : jsonVo.entrySet()) {
                String key = entry.getKey();
                String anVal = fieldAnMap.get(key);
                Object val = jsonVo.get(key);
                if ("_id".equalsIgnoreCase(anVal)) {
                    id = val;
                    continue;
                }
                if ("join_field".equalsIgnoreCase(anVal)) {
                    parentId = checkHasSon(val);
                }
                if (StringUtils.isNotEmpty(anVal)) {
                    jsonObject.put(anVal, JSON.toJSON(val));
                }
            }
            
            JSONObject jsonIndexParent = createIndexJson(id, parentId);
            
            this.postBody =
                postBody.append(jsonIndexParent.toJSONString()).append("\n")
                    .append(jsonObject.toJSONString()).append("\n");
        } catch (Exception e) {
            log.error("====Insert.add exception: ", e);
        }
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
    
    /**
     * 判断是否为子数据插入
     *
     * @param val 值
     * @return 是子类数据则返回parentId(父数据ID) 否则返回null
     */
    private Object checkHasSon(Object val) {
        Object parentId = null;
        try {
            JSONObject tempJson = JSON.parseObject(JSON.toJSONString(val));
            parentId = tempJson.get("parent");
        } catch (Exception e) {
            log.debug("====Insert Object has parent.");
        }
        return parentId;
    }
    
    /**
     * 构建批量插入Index json串
     *
     * @param id 数据id
     * @param parentId 父数据id
     */
    private JSONObject createIndexJson(Object id, Object parentId) {
        JSONObject jsonIndex = new JSONObject();
        jsonIndex.put("_index", this.getIndexNameStr());
        jsonIndex.put("_type", "data");
        if (null != id) {
            jsonIndex.put("_id", id);
        }
        if (null != parentId) {
            jsonIndex.put("_routing", parentId);
        }
        JSONObject jsonIndexParent = new JSONObject();
        jsonIndexParent.put("index", jsonIndex);
        
        return jsonIndexParent;
    }
    
    
    public static Map<String, Field[]> map = new HashMap<>();
    
    /**
     * 对象转换为JSONObject对象
     */
    private JSONObject convertJSON(T vo) throws IllegalAccessException {
        
//        Field[] fields = map.get(vo.getClass().getName());
//        if (null == fields) {
//            fields = vo.getClass().getFields();
//            map.put(vo.getClass().getName(), fields);
//            System.out.println("=====put");
//        }

        Field[] fields = vo.getClass().getFields();
        
        JSONObject jsonObject = new JSONObject();
        for (Field field : fields) {
            jsonObject.put(field.getName(), field.get(vo));
        }
        
        return jsonObject;
    }
    
}
