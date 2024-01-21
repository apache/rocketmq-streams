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
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.streams.common.channel.AbstractChannel;
import org.apache.rocketmq.streams.common.channel.sink.AbstractSink;
import org.apache.rocketmq.streams.common.channel.sink.ISink;
import org.apache.rocketmq.streams.common.channel.source.AbstractSource;
import org.apache.rocketmq.streams.common.channel.source.AbstractUnreliableSource;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.checkpoint.CheckPointMessage;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.MessageOffset;
import org.apache.rocketmq.streams.common.utils.IPUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.graylog2.syslog4j.Syslog;
import org.graylog2.syslog4j.SyslogConfigIF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyslogChannel extends AbstractChannel implements ISyslogRouter {
    private static final Logger LOGGER = LoggerFactory.getLogger(SyslogChannel.class);
    protected transient List<String> ipList = new ArrayList<>();
    //消息发送源的ip或域名列表，支持ip范围-192.168.1.1-192.168.1.10，ip端-192.168.0.0/22，和正则表达式
    protected String protol = SyslogChannelManager.UDP;//syslog支持的协议
    protected boolean syslogClientInit = true;
    protected String serverIp = IPUtil.getLocalIP();//需要发送给哪个syslog server，只有需要用到client进行日志发送时需要
    protected int port;//需要发送给哪个syslog port，只有需要用到client进行日志发送时需要
    protected String deviceType;
    protected String keywords;
    protected String ipListStr;

    private transient org.graylog2.syslog4j.SyslogIF syslogClient;

    public SyslogChannel() {
    }

    public SyslogChannel(String... ipList) {
        if (ipList != null) {
            for (String ip : ipList) {
                this.ipList.add(ip);
            }
        }

    }

    public SyslogChannel(String serverIp, int port) {
        this.serverIp = serverIp;
        this.port = port;
    }

    @Override
    protected ISink createSink() {
        this.sink = new AbstractSink() {
            @Override
            protected boolean batchInsert(List<IMessage> messages) {
                if (messages == null || !syslogClientInit) {
                    return true;
                }
                for (IMessage msg : messages) {
                    String message = null;
                    int level = 0;
                    try {

                        Integer tmp = msg.getMessageBody().getInteger("level");
                        if (tmp != null) {
                            level = tmp;
                        }
                        String encode = AbstractSource.CHARSET;
                        if (AbstractSource.class.isInstance(source)) {
                            AbstractSource abstractSource = (AbstractSource) source;
                            encode = abstractSource.getEncoding();
                        }
                        message = URLDecoder.decode(msg.getMessageValue().toString(), encode);
                        syslogClient.getConfig().setLocalName(IPUtil.getLocalIP());
                        syslogClient.getConfig().setSendLocalTimestamp(true);
                        syslogClient.getConfig().setSendLocalName(true);//如果这个值是false，需要确保json数据无空格
                        if ("127.0.0.1".equals(syslogClient.getConfig().getHost())) {
                            //本机测试必须设置，否则ip地址变成了127.0.0.1,如果是远端server，必须注释掉这一行，否则server发生覆盖
                            syslogClient.getConfig().setHost(IPUtil.getLocalIP());
                        }

                    } catch (Exception e) {
                        SyslogChannel.LOGGER.error("syslogClient decode message error " + msg.getMessageValue().toString(), e);
                    }
                    syslogClient.log(level, message);
                }
                syslogClient.flush();
                return true;
            }
        };
        return this.sink;
    }

    @Override
    protected ISource createSource() {
        this.source = new AbstractUnreliableSource() {
            @Override
            protected boolean startSource() {
                SyslogChannelManager.start(protol);
                return true;
            }

            @Override protected void destroySource() {

            }
        };
        return this.source;
    }

    @Override
    protected boolean initConfigurable() {
        boolean success = super.initConfigurable();
        if (SyslogChannelManager.TCP.equals(protol)) {
            SyslogChannelManager.registeTCP(this);
        } else if (SyslogChannelManager.UDP.equals(protol)) {
            SyslogChannelManager.registeUDP(this);
        }
        try {
            if (ipListStr != null) {
                String[] values = ipListStr.split(";");
                for (String value : values) {
                    ipList.add(value);
                }
            }
            syslogClient = Syslog.getInstance(protol);
            SyslogConfigIF config = syslogClient.getConfig();
            config.setHost(serverIp);
            config.setPort((protol.equals(SyslogChannelManager.UDP)) ? SyslogChannelManager.udpPort : SyslogChannelManager.tcpPort);
        } catch (Throwable throwable) {
            LOGGER.warn("syslogClient client init fail " + throwable);
            syslogClientInit = false;
        }

        return success;
    }

    public AbstractContext doReceiveMessage(JSONObject message) {
        return ((AbstractUnreliableSource) source).doReceiveMessage(message);
    }

    @Override
    public boolean match(String hostOrIp) {
        if (hostOrIp == null || ipList == null || ipList.size() == 0) {
            return false;
        }
        for (String ip : ipList) {
            if (StringUtil.isEmpty(ip)) {
                continue;
            }
            if (ip.equals(hostOrIp)) {
                return true;
            } else if (ip.indexOf("-") != -1) {
                boolean match = IPUtil.ipInSection(hostOrIp, ip);
                if (match) {
                    return true;
                }
            } else if (ip.indexOf("/") != -1) {
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
        return false;
    }

    public void setTCPProtol() {
        this.protol = SyslogChannelManager.TCP;
    }

    public void setUDPProtol() {
        this.protol = SyslogChannelManager.UDP;
    }

    public boolean isSyslogClientInit() {
        return syslogClientInit;
    }

    public void setSyslogClientInit(boolean syslogClientInit) {
        this.syslogClientInit = syslogClientInit;
    }

    @Override
    public void destroy() {
        super.destroy();
    }

    public void addIps(String... ips) {
        if (ips == null) {
            return;
        }
        this.ipList.addAll(Arrays.asList(ips));
    }

    public String getProtol() {
        return protol;
    }

    public void setProtol(String protol) {
        this.protol = protol;
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

    @Override
    public boolean equals(Object object) {
        if (object == this) {
            return true;
        }
        if (object instanceof SyslogChannel) {
            SyslogChannel other = (SyslogChannel) object;
            if (!this.protol.equals(other.protol)) {
                return false;
            }
            if (CollectionUtils.isEmpty(this.ipList) && CollectionUtils.isEmpty(other.ipList)) {
                return true;
            }
            if (CollectionUtils.isEmpty(this.ipList) || CollectionUtils.isEmpty(other.ipList)) {
                return false;
            }
            Collections.sort(other.ipList);
            Collections.sort(this.ipList);
            String origin = StringUtils.join(this.ipList.toArray(), ",");
            String another = StringUtils.join(((SyslogChannel) object).ipList.toArray(), ",");
            return origin.equals(another);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + (protol == null ? 0 : protol.hashCode());
        result = 31 * result + (ipList == null ? 0 : getListHashCode(ipList));
        return result;
    }

    private int getListHashCode(List<String> list) {
        if (list.size() == 0) {
            return 0;
        }
        int hash = 0;
        for (String s : list) {
            hash += s == null ? 0 : s.hashCode();
        }
        return hash;
    }

    public String getDeviceType() {
        return deviceType;
    }

    public void setDeviceType(String deviceType) {
        this.deviceType = deviceType;
    }

    public String getKeywords() {
        return keywords;
    }

    public void setKeywords(String keywords) {
        this.keywords = keywords;
    }

    @Override
    public Map<String, MessageOffset> getFinishedQueueIdAndOffsets(CheckPointMessage checkPointMessage) {
        return sink.getFinishedQueueIdAndOffsets(checkPointMessage);
    }

    @Override
    public boolean flushMessage(List<IMessage> messages) {
        return sink.flushMessage(messages);
    }

    @Override public String createCheckPointName() {
        return getName();
    }

    public String getIpListStr() {
        return ipListStr;
    }

    public void setIpListStr(String ipListStr) {
        this.ipListStr = ipListStr;
    }
}
