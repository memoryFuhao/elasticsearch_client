package com.elasticsearch.common.util;

import java.io.IOException;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

@Slf4j
public class HttpClientUtil {
    
    private static final String EN_CODED = "UTF-8";
    private static CloseableHttpClient httpClient = null;
    private static RequestConfig requestConfig = null;
    
    static {
        httpClient = HttpClients.createDefault();
        requestConfig = RequestConfig.custom()
            .setSocketTimeout(10000)// 请求获取数据的超时时间(即响应时间)，单位毫秒。
            .setConnectTimeout(10000)// 设置连接超时时间，单位毫秒。
            .setConnectionRequestTimeout(5000)// 设置从connect Manager(连接池)获取Connection 超时时间，单位毫秒。
            .build();
    }
    
    public static String doPost(String url, String JSONBody, Map<String, String> headerMap) {
        HttpPost httpPost = new HttpPost(url);
        addRequestHeader(httpPost, headerMap);
        
        if (StringUtils.isNotEmpty(JSONBody)) {
            httpPost.setEntity(new StringEntity(JSONBody, EN_CODED));
        }
        
        String responseContent = execute(httpPost);
        return responseContent;
    }
    
    public static String doGet(String url, Map<String, String> headerMap) {
        HttpGet httpGet = new HttpGet(url);
        addRequestHeader(httpGet, headerMap);
        
        String responseContent = execute(httpGet);
        return responseContent;
    }
    
    public static String doDelete(String url, Map<String, String> headerMap) {
        HttpDelete httpDelete = new HttpDelete(url);
        addRequestHeader(httpDelete, headerMap);
        
        String responseContent = execute(httpDelete);
        return responseContent;
    }
    
    /**
     * 执行http请求
     *
     * @param httpRequestBase 请求对象类型
     * @return HttpEntity 字符串
     */
    private static String execute(HttpRequestBase httpRequestBase) {
        String responseContent = null;
        CloseableHttpResponse response = null;
        try {
            response = httpClient.execute(httpRequestBase);
            HttpEntity entity = response.getEntity();
            responseContent = EntityUtils.toString(entity, EN_CODED);
        } catch (Exception e) {
            log.error("====HttpClientUtil.execute() exception:", e);
        } finally {
            try {
                response.close();
            } catch (IOException e) {
                log.error("====response.close() exception:", e);
            }
        }
        return responseContent;
    }
    
    
    /**
     * 添加请求头信息
     *
     * @param httpRequestBase 请求对象类型
     * @param para header参数
     */
    private static void addRequestHeader(HttpRequestBase httpRequestBase,
        Map<String, String> para) {
        if (MapUtils.isNotEmpty(para)) {
            for (Map.Entry<String, String> entry : para.entrySet()) {
                httpRequestBase.addHeader(entry.getKey(), entry.getValue());
            }
        }
        httpRequestBase.setConfig(requestConfig);
        httpRequestBase.addHeader("Content-Type", "application/json; charset=UTF-8");
    }
    
}
