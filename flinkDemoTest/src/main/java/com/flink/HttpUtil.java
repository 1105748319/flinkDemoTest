package com.flink;


import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.CharsetUtils;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

/**
 * HttpUtil
 * @author zhb
 * @date 2020/04/20
 */
public class HttpUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpUtil.class);
    private static PoolingHttpClientConnectionManager cm;
    private static RequestConfig requestConfig;
    public static final String CHAR_SET = "UTF-8";
    /**  最大连接数400. */
    private static int MAX_CONNECTION_NUM = 400;
    /** 单路由最大连接数80 */
    private static int MAX_PER_ROUTE = 80;
    /** 向服务端请求超时时间设置(单位:毫秒) */
    private static int SERVER_REQUEST_TIME_OUT = 60000;
    /** 服务端响应超时时间设置(单位:毫秒) */
    private static int SERVER_RESPONSE_TIME_OUT = 90000;
    static {
        try {
            cm = getPoolingHttpClientConnectionManager();
            requestConfig = RequestConfig.custom().setConnectionRequestTimeout(SERVER_REQUEST_TIME_OUT).setSocketTimeout(SERVER_REQUEST_TIME_OUT).setConnectTimeout(SERVER_RESPONSE_TIME_OUT).build();
        } catch (Exception e) {
            LOGGER.error("", e);
        }
        /** 定时处理过期的连接和空闲连接. */
        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {
                // 关闭过期的连接
                cm.closeExpiredConnections();
                // 关闭空闲时间超过30秒的连接
                cm.closeIdleConnections(30, TimeUnit.SECONDS);
            }
        }, 50000, 50000);
    }

    public static PoolingHttpClientConnectionManager getPoolingHttpClientConnectionManager () {
        try {
            SSLContextBuilder sslContextBuilder = new SSLContextBuilder();
            sslContextBuilder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
            SSLConnectionSocketFactory socketFactory = new SSLConnectionSocketFactory(sslContextBuilder.build());
            Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory> create()
                    .register("https", socketFactory)
                    .register("http", new PlainConnectionSocketFactory())
                    .build();
            PoolingHttpClientConnectionManager clientConnectionManager = new PoolingHttpClientConnectionManager(socketFactoryRegistry);
            clientConnectionManager.setMaxTotal(MAX_CONNECTION_NUM);
            clientConnectionManager.setDefaultMaxPerRoute(MAX_PER_ROUTE);
            return clientConnectionManager;
        } catch (Exception e) {
            LOGGER.error("", e);
            return null;
        }
    }

    /**
     * @description 获取一个http连接
     * @return
     */
    private static CloseableHttpClient getHttpClient(RequestConfig config) {
        CloseableHttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(config).setConnectionManager(cm).build();
        return httpClient;
    }

    /**
     * Https post请求
     * @param url
     * @param header
     * @param params
     * @return
     */
    public static HttpBean getHttpPostBean(String url, Map<String, String> header, Map<String, String> params) {
        HttpPost post = new HttpPost(url);
        post.setConfig(requestConfig);
        CloseableHttpResponse response = null;
        HttpBean httpBean = null;
        try {
            if (header != null && !header.isEmpty()) {
                for (String key : header.keySet()) {
                    post.addHeader(key, header.get(key));
                }
            }
            List<NameValuePair> paramList = new ArrayList<>();
            if (params != null && params.size() > 0) {
                Iterator<Entry<String, String>> iterator = params.entrySet().iterator();
                while (iterator.hasNext()) {
                    Entry<String, String> elem = iterator.next();
                    paramList.add(new BasicNameValuePair(elem.getKey(), elem.getValue()));
                }
            }
            UrlEncodedFormEntity paramEntity = new UrlEncodedFormEntity(paramList, CharsetUtils.get(CHAR_SET));
            post.setEntity(paramEntity);
            response = getHttpClient(requestConfig).execute(post);
            HttpEntity entity = response.getEntity();
            httpBean = new HttpBean();
            Header[] headers = response.getAllHeaders();
            httpBean.setResponseHeader(headers);

            if (entity != null) {
                // 使用EntityUtils的toString方法，传递默认编码，在EntityUtils中的默认编码是ISO-8859-1
                String content = EntityUtils.toString(entity, CHAR_SET);
                httpBean.setResponseContent(content);
            }
        } catch (Exception e) {
            LOGGER.error("", e);
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (Exception e) {
                    LOGGER.error("", e);
                }
            }
            post.abort();
        }
        return httpBean;
    }

    /**
     * Https post请求
     * @param url
     * @param header
     * @param params
     * @return
     */
    public static String post(String url, Map<String, String> header, Map<String, String> params) {
        HttpBean httpBean = getHttpPostBean(url, header, params);
        return httpBean != null ? httpBean.getResponseContent() : null;
    }

    /**
     * Https post请求
     * @param url
     * @param params
     * @return
     */
    public static String post(String url, Map<String, String> params) {
        HttpBean httpBean = getHttpPostBean(url, null, params);
        return httpBean != null ? httpBean.getResponseContent() : null;
    }

    /**
     * Https get请求
     * @param url
     * @return
     */
    public static String get(String url) {
        return get(url, null, null, null);
    }

    /**
     * Https get请求
     * @param url
     * @param charSet
     * @return
     */
    public static String get(String url, String charSet) {
        return get(url, null, null, charSet);
    }

    /**
     * Https get请求
     * @param url
     * @param header
     * @return
     */
    public static String get(String url, Map<String, String> header) {
        return get(url, header, null, null);
    }

    public static HttpBean getHttpGetBean(String url) {
        HttpGet get = new HttpGet(url);
        get.setConfig(requestConfig);
        CloseableHttpResponse response = null;
        String content = null;
        HttpBean httpBean = null;
        try {
            response = getHttpClient(requestConfig).execute(get);
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                content = EntityUtils.toString(entity, CHAR_SET);
            }

            httpBean = new HttpBean();
            Header[] headers = response.getAllHeaders();
            httpBean.setResponseHeader(headers);
            httpBean.setResponseContent(content);
        } catch (Exception e) {
            LOGGER.error("", e);
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (Exception e) {
                    LOGGER.error("", e);
                }
            }
            get.abort();
        }
        return httpBean;
    }

    /**
     * Https get请求
     * @param url
     * @param header
     * @param params
     * @return
     */
    public static String get(String url, Map<String, String> header, Map<String, String> params, String charSet) {
        if (params != null && !params.isEmpty()) {
            String str = "";
            for (String key : params.keySet()) {
                str += ("".equals(str) ? "?" : "&") + key + "=" + params.get(key);
            }
            url += str;
        }
        HttpGet get = new HttpGet(url);
        get.setConfig(requestConfig);
        CloseableHttpResponse response = null;
        String content = null;
        try {
            if (header != null && !header.isEmpty()) {
                for (String key : header.keySet()) {
                    get.addHeader(key, header.get(key));
                }
            }
            response = getHttpClient(requestConfig).execute(get);
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                content = EntityUtils.toString(entity, charSet == null ? CHAR_SET : charSet);
            }
        } catch (Exception e) {
            LOGGER.error("", e);
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (Exception e) {
                    LOGGER.error("", e);
                }
            }
            get.abort();
        }
        return content;
    }

    /**
     * @description HTTP POST
     * @param url
     * @param msg
     * @return
     */
    public static String post(String url, String msg) {
        HttpPost postMethod = new HttpPost(url);
        postMethod.setConfig(requestConfig);
        CloseableHttpResponse response = null;
        String content = null;
        try {
            // 从接过过来的代码转换为UTF-8的编码
            HttpEntity stringEntity = new StringEntity(msg, CharsetUtils.get("UTF-8"));
            postMethod.setEntity(stringEntity);
            postMethod.addHeader("Content-Type", "application/json;charset=UTF-8");
            response = getHttpClient(requestConfig).execute(postMethod);
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                // 使用EntityUtils的toString方法，传递默认编码，在EntityUtils中的默认编码是ISO-8859-1
                content = EntityUtils.toString(entity, "UTF-8");
            }
        } catch (Exception e) {
            LOGGER.error("", e);
        } finally {
            if (response != null) {
                try {
                    response.close();;
                } catch (IOException e) {
                    LOGGER.error("", e);
                }
            }
            postMethod.abort();
        }
        return content;
    }

    public static String combineQuery(Map<String, String> params) {
        if (params != null && !params.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for (String key : params.keySet()) {
                sb.append("".equals(sb.toString()) ? "?" : "&").append(key).append("=").append(params.get(key));
            }
            return sb.toString();
        } else {
            return "";
        }
    }

    public static class HttpBean {
        private String responseContent;
        private Header[] responseHeader;

        public String getResponseContent() {
            return responseContent;
        }

        public void setResponseContent(String responseContent) {
            this.responseContent = responseContent;
        }

        public Header[] getResponseHeader() {
            return responseHeader;
        }

        public void setResponseHeader(Header[] responseHeader) {
            this.responseHeader = responseHeader;
        }


    }
    public static void main(String[] args) {
        try {
            // 组装请求头
            Map<String, String> header = new HashMap<>(16);
            header.put("Content-Type", "application/json; charset=utf-8");

            // 组装请求参数
            Map<String, String> params = new HashMap<>(16);
            //params.put("recordCount", "10");


            // 登录
            String loginUrl = "http://192.168.120.192:8090/kafkaMsg/mysql/getData?recordCount=20";
            String result1 = HttpUtil.get(loginUrl);
            System.out.println(result1);
			List<Info> userInfos = JsonUtil.fromJsonArray(result1, Info.class);
			System.out.println(userInfos.size());
            //获取ticket

        } catch (Exception e) {

        }
    }
}
