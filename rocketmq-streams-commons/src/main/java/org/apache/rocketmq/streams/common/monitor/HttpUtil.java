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
package org.apache.rocketmq.streams.common.monitor;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Consts;
import org.apache.http.Header;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;

public class HttpUtil {

    public static final int NORMAL_STATUES = 200;
    public static final String CHARSET = "UTF-8";
    public static final int TIMOUT = 10000;
    public static final int CONNECT_TIMOUT = 10000;
    private static CloseableHttpClient httpclient;
    protected String accessId;
    protected String accessIdSecret;
    protected String endPoint;

    public HttpUtil(String accessId, String accessIdSecret, String endPoint) {
        this.accessId = accessId;
        this.accessIdSecret = accessIdSecret;
        this.endPoint = endPoint;
        init();
    }

    public static String getContent(String url) {
        return getContent(url, null);
    }

    public static String getContent(String url, Header... headers) {
        try {
            return EntityUtils.toString(get(url, headers).getEntity(), CHARSET);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String deleteContent(String url) {
        return deleteContent(url, null);
    }

    public static String deleteContent(String url, Header... headers) {
        try {
            return EntityUtils.toString(delete(url, headers).getEntity(), CHARSET);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String postContent(String url, String body) {
        return postContent(url, body, null);
    }

    public static String patchContent(String url, String body) {
        return patchContent(url, body, null);
    }

    public static String postContent(String url, String body, Header... headers) {
        try {
            return EntityUtils.toString(post(url, body, headers).getEntity(), CHARSET);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String patchContent(String url, String body, Header... headers) {
        try {
            return EntityUtils.toString(patch(url, body, headers).getEntity(), CHARSET);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static CloseableHttpResponse get(String url) {
        return get(url);
    }

    public static CloseableHttpResponse delete(String url) {
        return delete(url);
    }

    public static CloseableHttpResponse get(String url, Header... headers) {
        try {
            HttpGet httpGet = new HttpGet(url);
            if (headers != null && headers.length > 0) {
                for (Header header : headers) {
                    httpGet.addHeader(header);
                }
            }
            return httpclient.execute(httpGet);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static CloseableHttpResponse delete(String url, Header... headers) {
        try {
            HttpDelete httpDelete = new HttpDelete(url);
            if (headers != null && headers.length > 0) {
                for (Header header : headers) {
                    httpDelete.addHeader(header);
                }
            }
            return httpclient.execute(httpDelete);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static CloseableHttpResponse post(String url, String body) {
        return post(url, body);
    }

    public static CloseableHttpResponse patch(String url, String body) {
        return patch(url, body, null);
    }

    public static CloseableHttpResponse post(String url, String body, Header... headers) {
        try {
            HttpPost httpPost = new HttpPost(url);
            StringEntity stringEntity = new StringEntity(body, CHARSET);
            stringEntity.setContentEncoding(CHARSET);
            stringEntity.setContentType("application/json");
            httpPost.setEntity(stringEntity);
            if (headers != null && headers.length > 0) {
                for (Header header : headers) {
                    httpPost.addHeader(header);
                }
            }
            return httpclient.execute(httpPost);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static CloseableHttpResponse patch(String url, String body, Header... headers) {
        try {
            HttpPatch httpPatch = new HttpPatch(url);
            StringEntity stringEntity = new StringEntity(body, CHARSET);
            stringEntity.setContentEncoding(CHARSET);
            stringEntity.setContentType("application/json");
            httpPatch.setEntity(stringEntity);
            if (headers != null && headers.length > 0) {
                for (Header header : headers) {
                    httpPatch.addHeader(header);
                }
            }
            return httpclient.execute(httpPatch);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String replaceSpace(String str) {
        if (StringUtils.isNotBlank(str)) {
            return str.replace(" ", "").replace("\r", "").replace("\n", "").replace("\r\n", "");
        }
        return str;
    }

    private void init() {
        RequestConfig.Builder configBuilder = RequestConfig.custom();
        configBuilder.setConnectionRequestTimeout(CONNECT_TIMOUT);
        configBuilder.setConnectTimeout(CONNECT_TIMOUT);
        configBuilder.setSocketTimeout(TIMOUT);
        SSLConnectionSocketFactory sslsf = null;
        try {
            SSLContext sslcontext = SSLContexts.custom().loadTrustMaterial(new TrustStrategy() {
                @Override
                public boolean isTrusted(X509Certificate[] chain, String authType) throws CertificateException {
                    return true;
                }
            }).build();
            sslsf = new SSLConnectionSocketFactory(sslcontext, new HostnameVerifier() {
                @Override
                public boolean verify(String s, SSLSession sslSession) {
                    return true;
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        ConnectionConfig connectionConfig = ConnectionConfig.custom().setCharset(Consts.UTF_8).build();
        Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create().register("https", sslsf).register("http", new PlainConnectionSocketFactory()).build();
        PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager(socketFactoryRegistry);
        connManager.setDefaultConnectionConfig(connectionConfig);
        connManager.setMaxTotal(500);
        connManager.setDefaultMaxPerRoute(50);
        HttpClientBuilder clientBuilder = HttpClients.custom();

        clientBuilder.setDefaultRequestConfig(configBuilder.build());
        clientBuilder.setSSLSocketFactory(sslsf);
        clientBuilder.setConnectionManager(connManager);

        httpclient = clientBuilder.build();
    }
}

