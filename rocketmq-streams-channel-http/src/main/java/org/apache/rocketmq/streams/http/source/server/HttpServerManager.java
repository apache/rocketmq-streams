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
package org.apache.rocketmq.streams.http.source.server;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.http.source.HttpSource;

/**
 * receive http(s) post data
 **/
public class HttpServerManager {
    private static final Log LOG = LogFactory.getLog(HttpServerManager.class);
    protected transient static HttpServer HTTP_SERVER = new HttpServer();//http server，全局只有一个
    protected transient static HttpServer HTTPS_SERVER = new HttpServer();//https server 全局只有一个
    protected transient static AtomicBoolean isHttpStart = new AtomicBoolean(false);//只启动一次
    protected transient static AtomicBoolean isHttpsStart = new AtomicBoolean(false);//只启动一次

    public static void register(HttpSource channel, boolean isHttps) {
        if (isHttps) {
            HTTPS_SERVER.register(channel);
        } else {
            HTTP_SERVER.register(channel);
        }
    }

    public static void startServer(boolean isHttps) {
        if (isHttps) {
            if (isHttpsStart.compareAndSet(false, true)) {
                HTTPS_SERVER.setUseHttps(true);
                HTTPS_SERVER.setPort(443);
                HTTPS_SERVER.init();
                HTTPS_SERVER.startSource();
            }
        } else {
            if (isHttpStart.compareAndSet(false, true)) {
                HTTP_SERVER.setPort(8000);
                HTTP_SERVER.setUseHttps(false);
                HTTP_SERVER.init();
                HTTP_SERVER.startSource();
            }
        }
    }
}