/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.streams.openapi.executor;

import com.alibaba.fastjson.JSONObject;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import javax.net.ssl.SSLContext;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.rocketmq.streams.openapi.utils.ParamUtil;

public class HttpOpenAPIExecutor extends AbstractOpenAPIExecutor {
    protected String proxyHttp;//主要用于日常环境测试，正式场景不需要
    protected int port = 0;//代理端口，主要用于测试
    protected transient String signatureMethod = "HMAC-SHA1";
    //唯一随机数，用于防止网络重放攻击。用户在不同请求间要使用不同的随机数值
    protected transient String signatureNonce = UUID.randomUUID().toString().replace("-", "").toUpperCase() + "fddsfsddsffsdfds";
    //签名算法版本 固定值
    protected transient String signatureVersion = "1.0";

    public String createURL(JSONObject businessParameters) {
        TreeMap<String, Object> paraMap = new TreeMap<>();
        paraMap.putAll(this.appBusinessParameters(businessParameters));
        //公共参数
        paraMap.put("Format", format);
        paraMap.put("AccessKeyId", accessKeyId);
        paraMap.put("Action", action);
        paraMap.put("SignatureMethod", signatureMethod);
        paraMap.put("SignatureNonce", signatureNonce);
        paraMap.put("Version", version);
        paraMap.put("SignatureVersion", signatureVersion);
        Iterator<Map.Entry<String, Object>> it = paraMap.entrySet().iterator();
        String url = ParamUtil.getUrl(domain, accessSecret, method, paraMap);
        return url;
    }

    @Override
    protected JSONObject doInvoke(JSONObject businessParameters) {
        JSONObject paras = appBusinessParameters(businessParameters);
        return invoke(paras);
    }

    @Override
    protected JSONObject invoke(JSONObject businessParameters) {
        RequestConfig defaultConfig = RequestConfig.custom().build();
        if (!"".equals(proxyHttp) && port != 0) {
            HttpHost proxy = new HttpHost(proxyHttp, port, "http");
            defaultConfig = RequestConfig.custom().setProxy(proxy).build();
        }

        SSLContext sslContext = null;
        CloseableHttpResponse response = null;
        try {
            sslContext = SSLContextBuilder.create().useProtocol(SSLConnectionSocketFactory.SSL).loadTrustMaterial((x,
                y) -> true).build();
        } catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException e) {
            e.printStackTrace();
        }
        CloseableHttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(defaultConfig).setSSLContext(sslContext).setSSLHostnameVerifier((x, y) -> true).build();
        String url = createURL(businessParameters);
        HttpGet get = new HttpGet(url);
        String result = "";
        try {
            response = httpClient.execute(get);
            HttpEntity entity = response.getEntity();
            result = EntityUtils.toString(entity, "utf-8");

        } catch (IOException e) {
            throw new RuntimeException("send open api http request error " + url);

        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return JSONObject.parseObject(result);

    }

    public String getProxyHttp() {
        return proxyHttp;
    }

    public void setProxyHttp(String proxyHttp) {
        this.proxyHttp = proxyHttp;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}
