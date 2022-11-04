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
package org.apache.rocketmq.streams.es.sink;

import java.io.IOException;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zhangliang
 * <p>
 * 2021-04-16 17:20
 */
public class EsClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(EsClient.class);

    //private static final Logger logger = LogManager.getLogger(EsClient.class);

    private RestHighLevelClient esClient;

    private Object object = new Object();

    private static final int socketTimeOut = 5 * 60 * 1000;

    private static final int connectTimeOut = 5 * 60 * 1000;

    private static final int connectionRequestTimeOut = 5 * 60 * 1000;

    private static final boolean needAuth = true;

    private String host;

    private String port;

    private String username;

    private String password;

    private static String SCHEME = "http";

    public EsClient(String host, String port, String username, String password) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
    }

    public void init() {
        getEsClient();
    }

    public RestHighLevelClient getEsClient() {
        if (esClient == null) {
            synchronized (object) {
                if (esClient == null) {
                    esClient = createClient();
                }
            }
        }
        return esClient;
    }

    private RestHighLevelClient createClient() {
        LOGGER.info("esClient createClient host=" + host + " port=" + port);
        try {
            //解决netty冲突问题
            System.setProperty("es.set.netty.runtime.available.processors", "false");
            RestClientBuilder builder = RestClient.builder(new HttpHost(host, Integer.parseInt(port), SCHEME));
            builder.setRequestConfigCallback((config) -> {
                config.setConnectionRequestTimeout(connectionRequestTimeOut);
                config.setConnectTimeout(connectTimeOut);
                config.setSocketTimeout(socketTimeOut);
                return config;
            });
            if (needAuth) {
                final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
                builder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
            }
            esClient = new RestHighLevelClient(builder);
        } catch (Exception e) {
            LOGGER.error("esClient createClient host=" + host + " port=" + port + " error=" + e.getMessage(), e);
            throw e;
        }
        return esClient;
    }

    public void rebuildEsCilent() {
        closeClient();
        getEsClient();
    }

    private void closeClient() {
        LOGGER.error("esClient closeClient host=" + host + " port=" + port);
        if (esClient != null) {
            try {
                esClient.close();
                esClient = null;
                LOGGER.error("esClient closeClient success host=" + host + " port=" + port);
            } catch (IOException e) {
                LOGGER.error("esClient closeClient failed host=" + host + " port=" + port + " error=" + e.getMessage(), e);
            }
        }
    }
}
