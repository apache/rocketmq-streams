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
package org.apache.rocketmq.streams.http.source;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.rocketmq.streams.http.source.server.HttpServerManager;
import org.apache.rocketmq.streams.common.channel.source.AbstractUnreliableSource;
import org.apache.rocketmq.streams.common.utils.IPUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * receive http(s) post data
 **/
public class HttpSource extends AbstractUnreliableSource {
    private static final Log LOG = LogFactory.getLog(HttpSource.class);
    protected boolean isHttps = false;//发送的请求是否是https的

    /**
     * 下面三个可以都配置，或者只配置一个，只要有匹配上的请求，都会被这个channel处理
     */

    //消息发送源的ip或域名列表，支持ip范围-192.168.1.1-192.168.1.10，ip端-192.168.0.0/22，和正则表达式
    protected List<String> ipList = new ArrayList<>();
    protected List<String> rootPath = new ArrayList<>();//请求的跟path，支持正则和列表
    protected String routeLabel;//可以在请求的参数中加入一个固定的参数，这个值是参数名称
    protected String routeValue;

    public boolean match(String hostOrIp, String query, String uri) {
        String routePara = routeLabel + "=" + routeValue;
        if (StringUtil.isNotEmpty(query) && query.contains(routePara)) {
            return true;
        }
        if (StringUtil.isNotEmpty(uri)) {
            for (String path : rootPath) {
                if (uri.startsWith(path)) {
                    return true;
                }
                if (StringUtil.matchRegex(uri, path)) {
                    return true;
                }
            }
        }
        if (StringUtil.isNotEmpty(hostOrIp)) {
            for (String ip : ipList) {
                if (StringUtil.isEmpty(ip)) {
                    continue;
                }
                if (ip.equals(hostOrIp)) {
                    return true;
                } else if (ip.contains("-")) {
                    boolean match = IPUtil.ipInSection(hostOrIp, ip);
                    if (match) {
                        return true;
                    }
                } else if (ip.contains("/")) {
                    boolean match = IPUtil.isInRange(hostOrIp, ip);
                    if (match) {
                        return true;
                    }
                } else {
                    boolean match = StringUtil.matchRegex(hostOrIp, ip);
                    if (match) {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    @Override
    protected boolean initConfigurable() {
        boolean success = super.initConfigurable();
        HttpServerManager.register(this, isHttps);
        return success;
    }

    @Override
    protected boolean startSource() {
        HttpServerManager.startServer(isHttps);
        return true;
    }

    public boolean isHttps() {
        return isHttps;
    }

    public void setHttps(boolean https) {
        isHttps = https;
    }

    public List<String> getIpList() {
        return ipList;
    }

    public void setIpList(List<String> ipList) {
        this.ipList = ipList;
    }

    public List<String> getRootPath() {
        return rootPath;
    }

    public void setRootPath(List<String> rootPath) {
        this.rootPath = rootPath;
    }

    public String getRouteLabel() {
        return routeLabel;
    }

    public void setRouteLabel(String routeLabel) {
        this.routeLabel = routeLabel;
    }

    public String getRouteValue() {
        return routeValue;
    }

    public void setRouteValue(String routeValue) {
        this.routeValue = routeValue;
    }

    public void addIps(String... ips) {
        if (ips == null) {
            return;
        }
        Collections.addAll(this.ipList, ips);
    }

    public void addPath(String... paths) {
        if (paths == null) {
            return;
        }
        Collections.addAll(this.rootPath, paths);
    }
}