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

import com.alibaba.fastjson.JSONObject;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.channel.source.AbstractUnreliableSource;
import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.http.source.HttpSource;

public class HttpServer extends AbstractUnreliableSource {
    private static final Log LOG = LogFactory.getLog(HttpServer.class);
    private transient com.sun.net.httpserver.HttpServer server;
    @ENVDependence
    private int port = 8000;
    @ENVDependence
    private String serverIp = "localhost";
    @ENVDependence
    private int backlog = 10;
    @ENVDependence
    private int stopDelaySecond;
    private boolean useHttps = false;

    protected transient List<HttpSource> channels = new ArrayList<>();

    public HttpServer() {
        setJsonData(false);
        setMsgIsJsonArray(false);
    }

    public void register(HttpSource channel) {
        this.channels.add(channel);
    }

    @Override
    protected boolean initConfigurable() {
        try {

            if (useHttps) {
                HttpsServer httpsServer = HttpsServer.create(new InetSocketAddress(serverIp, port), backlog);
                SSLContext sslContext = SSLContext.getInstance("TLS");
                // initialise the keystore
                char[] password = "password".toCharArray();
                KeyStore ks = KeyStore.getInstance("JKS");
                InputStream fis = this.getClass().getResourceAsStream("/testkey.jks");
                ks.load(fis, password);
                // setup the key manager factory
                KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
                kmf.init(ks, password);
                // setup the trust manager factory
                TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
                tmf.init(ks);
                // setup the HTTPS context and parameters
                sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
                httpsServer.setHttpsConfigurator(new HttpsConfigurator(sslContext) {
                    @Override
                    public void configure(HttpsParameters params) {
                        try {
                            // initialise the SSL context
                            SSLContext context = getSSLContext();
                            SSLEngine engine = context.createSSLEngine();
                            params.setNeedClientAuth(false);
                            params.setCipherSuites(engine.getEnabledCipherSuites());
                            params.setProtocols(engine.getEnabledProtocols());

                            // Set the SSL parameters
                            SSLParameters sslParameters = context.getSupportedSSLParameters();
                            params.setSSLParameters(sslParameters);

                        } catch (Exception ex) {
                            LOG.error("Failed to create HTTPS port", ex);
                        }
                    }
                });
                server = httpsServer;
            } else {
                server = com.sun.net.httpserver.HttpServer.create(new InetSocketAddress(serverIp, port), backlog);
            }

        } catch (IOException e) {
            //            System.out.println(e);
            LOG.error("http channel init get io exception", e);
            return false;
        } catch (NoSuchAlgorithmException e) {
            //            System.out.println(e);
            LOG.error("http channel init https ssl context exception", e);
            return false;
        } catch (Exception e) {
            //            System.out.println(e);
            LOG.error("http channel init http cert exception", e);
            return false;
        }
        return true;
    }

    @Override
    protected boolean startSource() {
        ExecutorService cachedThreadPool = new ThreadPoolExecutor(maxThread, maxThread,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(1000));
        server.setExecutor(cachedThreadPool);
        server.createContext("/", new DataHandler());
        setReceiver((message, context) -> {
            String clientIp = message.getMessageBody().getString("clientIp");
            String query = message.getMessageBody().getString("query");
            String uri = message.getMessageBody().getString("uri");
            List<HttpSource> destroyChannels = new ArrayList<>();
            for (HttpSource channel : channels) {
                if (channel.match(clientIp, query, uri)) {
                    if (channel.isDestroy()) {
                        destroyChannels.add(channel);
                        continue;
                    }
                    channel.doReceiveMessage(message.getMessageBody());
                }
            }
            for (HttpSource httpChannel : destroyChannels) {
                channels.remove(httpChannel);
            }
            return message;
        });
        server.start();
        if (isUseHttps()) {
            LOG.info("https server is start");
        } else {
            LOG.info("http server is start");
        }

        return true;
    }

    public boolean isUseHttps() {
        return useHttps;
    }

    public void setUseHttps(boolean useHttps) {
        this.useHttps = useHttps;
    }

    public String getServerIp() {
        return serverIp;
    }

    public void setServerIp(String serverIp) {
        this.serverIp = serverIp;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getBacklog() {
        return backlog;
    }

    public void setBacklog(int backlog) {
        this.backlog = backlog;
    }

    public int getStopDelaySecond() {
        return stopDelaySecond;
    }

    public void setStopDelaySecond(int delaySecond) {
        this.stopDelaySecond = delaySecond;
    }

    private class DataHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            OutputStream os = exchange.getResponseBody();
            InputStream in = exchange.getRequestBody();
            try {
                JSONObject jsonObject = createHttpHeader(exchange);
                jsonObject.put("query", exchange.getRequestURI().getQuery());
                String data = getContent(in);
                jsonObject.put(IMessage.DATA_KEY, data);

                doReceiveMessage(jsonObject);
                String response = "{\"code\": \"200\", \"data\" :\"received\", \"message\" :\"\"}";
                exchange.sendResponseHeaders(200, 0);
                os.write(response.getBytes());
                os.flush();
            } catch (Exception e) {
                LOG.error("");
            } finally {
                if (os != null) {
                    os.close();
                }
                if (in != null) {
                    in.close();
                }
            }

        }
    }

    protected JSONObject createHttpHeader(HttpExchange exchange) {
        JSONObject header = new JSONObject();
        header.put("clientIp", exchange.getRemoteAddress().getAddress().getHostAddress());
        header.put("uri", exchange.getRequestURI().getPath());
        header.put("User-agent", exchange.getRequestHeaders().getFirst("User-agent"));
        header.put("Content-type", exchange.getRequestHeaders().getFirst("Content-type"));
        header.put("Host", exchange.getRequestHeaders().getFirst("Host"));
        header.put("method", exchange.getRequestMethod());
        return header;
    }

    /**
     * 从请求里获取内容
     *
     * @param in
     * @return
     * @throws IOException
     */
    protected String getContent(InputStream in) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        String line;
        StringBuilder stringBuilder = new StringBuilder();
        while ((line = reader.readLine()) != null) {
            stringBuilder.append(line);
        }
        return stringBuilder.toString();
    }
}