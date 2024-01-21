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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.rocketmq.streams.common.channel.sink.AbstractSink;
import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ESSinkOnlyChannel extends AbstractSink {
    private static final Logger LOGGER = LoggerFactory.getLogger(ESSinkOnlyChannel.class);

    private static final String PREFIX = "xxx";
    protected String esMsgId;
    @ENVDependence
    private String host;
    @ENVDependence
    private String port;
    private boolean needAuth = false;
    @ENVDependence
    private String authUsername;
    @ENVDependence
    private String authPassword;
    ;
    private int socketTimeOut = 5 * 60 * 1000;
    ;
    private int connectTimeOut = 5 * 60 * 1000;
    ;
    private int connectionRequestTimeOut = 5 * 60 * 1000;
    private String schema = "http";
    @ENVDependence
    private String esIndex;
    private String esIndexType = "log";
    private transient RestHighLevelClient client;

    public ESSinkOnlyChannel() {

    }

    public boolean isNeedAuth() {
        return needAuth;
    }

    public void setNeedAuth(boolean needAuth) {
        this.needAuth = needAuth;
    }

    public String getAuthUsername() {
        return authUsername;
    }

    public void setAuthUsername(String authUsername) {
        this.authUsername = authUsername;
    }

    public String getAuthPassword() {
        return authPassword;
    }

    public void setAuthPassword(String authPassword) {
        this.authPassword = authPassword;
    }

    @Override
    protected boolean initConfigurable() {
        super.initConfigurable();
        System.setProperty("es.set.netty.runtime.available.processors", "false");
        if (client == null) {
            RestClientBuilder builder = RestClient.builder(new HttpHost(host, Integer.parseInt(port), schema));
            builder.setRequestConfigCallback((config) -> {
                config.setConnectionRequestTimeout(connectionRequestTimeOut);
                config.setConnectTimeout(connectTimeOut);
                config.setSocketTimeout(socketTimeOut);
                return config;
            });
            if (needAuth) {
                final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(authUsername, authPassword));
                builder.setHttpClientConfigCallback(
                    httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
            }
            try {
                client = new RestHighLevelClient(builder);
            } catch (Exception e) {
                throw new RuntimeException("unknowhost exception ", e);
            }
        }
        return true;
    }

    private List<IndexRequest> generateRequests(List<IMessage> messages) {
        List<IndexRequest> requests = new ArrayList<>();
        messages.forEach(message -> {
            IndexRequest indexRequest = new IndexRequest(esIndex);
            Object object = message.getMessageValue();
            if (object != null && !(object instanceof Map)) {
                String str = object.toString();
                if (str.startsWith("{") && str.endsWith("}")) {
                    try {
                        JSONObject jsonObject = JSON.parseObject(str);
                        object = jsonObject;
                    } catch (Exception e) {
                        LOGGER.warn("the sink msg is not json, convert error");
                    }

                }
            }
            if (object instanceof Map) {
                indexRequest.source((Map<String, ?>) object);
                if (StringUtil.isNotEmpty(esMsgId)) {
                    Map map = (Map) object;
                    Object msgId = map.get(esMsgId);
                    if (msgId != null) {
                        indexRequest.id(msgId.toString());
                    }
                }

            } else {
                indexRequest.source(object.toString());
            }

            requests.add(indexRequest);
        });
        return requests;
    }

    @Override
    public boolean batchInsert(List<IMessage> messages) {
        BulkRequest bulkRequest = new BulkRequest();
        BulkResponse response = null;
        List<IndexRequest> requestList = generateRequests(messages);
        requestList.forEach(bulkRequest::add);
        try {
            response = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
            LOGGER.error("batch insert message to es exception " + e);
            return false;
        }

        LOGGER.info("esChannel sendLogs logSize=" + messages.size() + " response size"
            + response.getItems().length + " status " + response.status()
            + " cost=" + response.getTook() + " esIndex=" + esIndex + " host=" + host);
        return true;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getEsIndex() {
        return esIndex;
    }

    public void setEsIndex(String esIndex) {
        this.esIndex = esIndex;
    }

    public String getEsIndexType() {
        return esIndexType;
    }

    public void setEsIndexType(String esIndexType) {
        this.esIndexType = esIndexType;
    }

    public int getSize() {
        return this.messageCache.getMessageCount();
    }

    public int getSocketTimeOut() {
        return socketTimeOut;
    }

    public void setSocketTimeOut(int socketTimeOut) {
        this.socketTimeOut = socketTimeOut;
    }

    public int getConnectTimeOut() {
        return connectTimeOut;
    }

    public void setConnectTimeOut(int connectTimeOut) {
        this.connectTimeOut = connectTimeOut;
    }

    public int getConnectionRequestTimeOut() {
        return connectionRequestTimeOut;
    }

    public void setConnectionRequestTimeOut(int connectionRequestTimeOut) {
        this.connectionRequestTimeOut = connectionRequestTimeOut;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public RestHighLevelClient getClient() {
        return client;
    }

    public void setClient(RestHighLevelClient client) {
        this.client = client;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getEsMsgId() {
        return esMsgId;
    }

    public void setEsMsgId(String esMsgId) {
        this.esMsgId = esMsgId;
    }
}
