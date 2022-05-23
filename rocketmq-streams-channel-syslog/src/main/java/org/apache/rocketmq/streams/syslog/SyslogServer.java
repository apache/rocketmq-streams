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
package org.apache.rocketmq.streams.syslog;

import com.alibaba.fastjson.JSONObject;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.cache.softreference.impl.SoftReferenceCache;
import org.apache.rocketmq.streams.common.channel.source.AbstractUnreliableSource;
import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.UserDefinedMessage;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.common.utils.IPUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.graylog2.syslog4j.server.SyslogServerConfigIF;
import org.graylog2.syslog4j.server.SyslogServerEventIF;
import org.graylog2.syslog4j.server.SyslogServerIF;
import org.graylog2.syslog4j.server.SyslogServerSessionEventHandlerIF;
import org.graylog2.syslog4j.server.impl.net.tcp.TCPNetSyslogServerConfigIF;

public class SyslogServer extends AbstractUnreliableSource {
    private static final Log LOG = LogFactory.getLog(SyslogServer.class);
    @ENVDependence private String serverHost = IPUtil.getLocalAddress();
    @ENVDependence private String serverPort;
    private String protocol;
    private Integer ctimeout = DEFAULT_TIMEOUT;

    private transient SyslogServerIF syslogServer;

    private static final int DEFAULT_TIMEOUT = 10000;  //毫秒，只有tcp时有用

    private static final String MSG_ID_NULL = "MSG_ID_NULL";

    private volatile boolean isFinished = false;                                 // 如果消息被销毁，会通过这个标记停止消息的消费

    protected int timeout;
    /**
     * 注册路由信息，由主服务做路由分发
     */
    private transient List<SyslogChannel> routers = new ArrayList<>();

    @Override protected boolean initConfigurable() {

        if (SyslogChannelManager.TCP.equals(protocol)) {
            if (ctimeout == null) {
                timeout = DEFAULT_TIMEOUT;
            }
        }

        syslogServer = org.graylog2.syslog4j.server.SyslogServer.getInstance(protocol);
        SyslogServerConfigIF var3 = syslogServer.getConfig();
        var3.setHost(serverHost);
        var3.setPort(Integer.parseInt(serverPort));

        if (ctimeout != null) {
            if (var3 instanceof TCPNetSyslogServerConfigIF) {
                ((TCPNetSyslogServerConfigIF) var3).setTimeout(ctimeout);
            }
        }
        SyslogServerSessionEventHandlerIF var5 = new SyslogServerEventHandler();
        var3.addEventHandler(var5);
        setSingleType(true);//单消费者
        return super.initConfigurable();
    }

    @Override public boolean startSource() {
        setReceiver(new IStreamOperator() {
            @Override public Object doMessage(IMessage message, AbstractContext context) {
                String hostAddress = message.getMessageBody().getString("hostAddress");
                if(hostAddress==null){
                    return null;
                }
                List<SyslogChannel> syslogChannels = cache.get(hostAddress);
                LOG.info("receive syslog msg, ip is  " + hostAddress + " msg is " + message.getMessageBody());
                boolean hasMatch = false;
                if (syslogChannels == null) {
                    syslogChannels = new ArrayList<>();
                    for (SyslogChannel channel : routers) {//如果没有合适的路由消息，消息直接丢弃
                        if (channel.match(hostAddress)) {
                            syslogChannels.add(channel);
                        }
                    }
                }
                if (syslogChannels.size() > 0) {
                    cache.put(hostAddress, syslogChannels);
                    hasMatch = true;
                }
                if (!hasMatch) {
                    LOG.warn("the syslog msg had been discard, beacuse not match ip list, the ip is  " + hostAddress + ". the msg is " + message.getMessageBody());
                    return message;
                }
                for (SyslogChannel channel : syslogChannels) {
                    if (channel.isDestroy() == false) {
                        channel.doReceiveMessage(message.getMessageBody());
                    } else {
                        routers.remove(channel);
                    }
                }
                return message;
            }
        });
        SyslogServerIF serverIF=org.graylog2.syslog4j.server.SyslogServer.getThreadedInstance(protocol);
        return true;
    }

    private static final String PREFIX = "dipper.upgrade.channel.syslog.envkey";

    @Override public void destroy() {
        isFinished = true;
        if (syslogServer != null) {
            try {
                syslogServer.shutdown();
                Thread.sleep(30 * 1000);
            } catch (Exception e) {

            }

        }
    }

    public String getServerHost() {
        return serverHost;
    }

    public void setServerHost(String serverHost) {
        this.serverHost = serverHost;
    }

    public String getServerPort() {
        return serverPort;
    }

    public void setServerPort(String serverPort) {
        this.serverPort = serverPort;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public Integer getCtimeout() {
        return ctimeout;
    }

    public void setCtimeout(Integer ctimeout) {
        this.ctimeout = ctimeout;
    }

    public boolean isFinished() {
        return isFinished;
    }

    public void setFinished(boolean finished) {
        isFinished = finished;
    }

    private volatile transient SoftReferenceCache<String, List<SyslogChannel>> cache = new SoftReferenceCache<>();

    protected class SyslogServerEventHandler implements SyslogServerSessionEventHandlerIF {
        private static final long serialVersionUID = 6036415838696050746L;

        public SyslogServerEventHandler() {

        }

        @Override public void initialize(SyslogServerIF var1) {
        }

        @Override public Object sessionOpened(SyslogServerIF var1, SocketAddress var2) {
            return null;
        }

        @Override public void event(Object var1, SyslogServerIF var2, SocketAddress var3, SyslogServerEventIF var4) {

            String hostAddress = null;
            if (InetSocketAddress.class.isInstance(var3)) {
                InetSocketAddress address = (InetSocketAddress) var3;
                hostAddress = address.getAddress().getHostAddress();
            } else {
                String ipTmp = var3.toString().replace("/", "");
                int endIndx = ipTmp.indexOf(":");
                hostAddress = ipTmp.substring(0, endIndx);
            }

            String message = var4.getMessage();
            Date date = var4.getDate();
            if (date != null) {
                message = SyslogParser.parseDate(message, date);
            } else {
                date = DateUtil.getCurrentTime();
            }
            String lastMessage = SyslogParser.parseHost(message);
            String hostName = message.replace(lastMessage, "");
            if (StringUtil.isEmpty(hostName)) {
                hostName = null;
            }
            message = lastMessage;
            String tag = SyslogParser.parseTags(message);
            String pid = null;
            if (StringUtil.isNotEmpty(tag)) {
                message = message.replace(tag + ":", "").trim();
                if (tag.indexOf("[") != -1) {
                    int startIndex = tag.indexOf("[");
                    int endIndex = tag.indexOf("]");
                    pid = tag.substring(startIndex + 1, endIndex);
                    tag = tag.substring(0, startIndex);
                }

            }
            JSONObject msg = new JSONObject();
            msg.put("data",message);
            msg.put("facility", var4.getFacility());
            msg.put("hostName", hostName);
            msg.put("hostAddress", hostAddress);
            msg.put("level", var4.getLevel());
            msg.put("log_time", DateUtil.format(date));
            msg.put("tag", tag);
            msg.put("pid", pid);

            doReceiveMessage(msg);

        }

        @Override public void exception(Object var1, SyslogServerIF var2, SocketAddress var3, Exception var4) {
        }

        @Override public void sessionClosed(Object var1, SyslogServerIF var2, SocketAddress var3, boolean var4) {
        }

        @Override public void destroy(SyslogServerIF var1) {
        }
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public void clearCache() {
        SoftReferenceCache tmp = cache;
        cache = new SoftReferenceCache<>();
        tmp.clear();
    }

    public List<SyslogChannel> getRouters() {
        return routers;
    }
}
