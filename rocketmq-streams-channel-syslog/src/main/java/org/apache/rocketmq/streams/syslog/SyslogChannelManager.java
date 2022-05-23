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

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.graylog2.syslog4j.SyslogConstants;

public class SyslogChannelManager {
    public static final String TCP = SyslogConstants.TCP;//tcp协议名称
    public static final String UDP = SyslogConstants.UDP;//tcp协议名称

    public static final String TCP_PORT_PROPERTY_KEY = "dipper.syslog.server.tcp.port";//当需要改变端口值时，通过配置文件增加dipper.syslog.server.tcp.port=新端口的值
    public static final String UDP_PORT_PROPERTY_KEY = "dipper.syslog.server.udp.port";//当需要改变端口值时，通过配置文件增加dipper.syslog.server.tcp.port=新端口的值

    public  static int tcpPort = 12345;//syslog server默认端口
    public  static int udpPort = 12346;//syslog server默认端口

    private static AtomicBoolean tcpStart = new AtomicBoolean(false);//标记是否启动tcp server，只会启动一次
    private static AtomicBoolean updStart = new AtomicBoolean(false);//标记是否启动udp server，只会启动一次
    private static SyslogServer TCP_CHANNEL = new SyslogServer();//全局启动一个tcp 服务
    private static SyslogServer UDP_CHANNEL = new SyslogServer();//全局启动一个udp服务

    public static void registeTCP(SyslogChannel syslogRouter) {
        if (!TCP_CHANNEL.getRouters().contains(syslogRouter)) {
            TCP_CHANNEL.getRouters().add(syslogRouter);
            if (tcpPort == 12345&&syslogRouter.getPort()>0) {
                tcpPort = syslogRouter.getPort();
            }
        }
        if (tcpStart.get()) {//如果服务已经启动，再有新的channel注册，需要清空缓存，以便以新的channel能够被转发数据
            TCP_CHANNEL.clearCache();
        }
    }

    public static void registeUDP(SyslogChannel syslogRouter) {
        if (!UDP_CHANNEL.getRouters().contains(syslogRouter)) {
            if (udpPort == 12345) {
                udpPort = syslogRouter.getPort();
            }
            UDP_CHANNEL.getRouters().add(syslogRouter);
        }
        if (updStart.get()) {//如果服务已经启动，再有新的channel注册，需要清空缓存，以便以新的channel能够被转发数据
            UDP_CHANNEL.clearCache();
        }
    }

    public static void start(String protol) {

        if (TCP.equals(protol)) {

            startTCPServer();
        } else if (UDP.equals(protol)) {

            startUDPServer();
        }
    }

    protected static void startTCPServer() {
        if (tcpStart.compareAndSet(false, true)) {
            String value = ComponentCreator.getProperties().getProperty(TCP_PORT_PROPERTY_KEY);
            if (StringUtil.isNotEmpty(value)) {
                tcpPort = Integer.valueOf(value);
            }
            TCP_CHANNEL.setProtocol(TCP);
            ;
            TCP_CHANNEL.setServerPort(tcpPort + "");
            TCP_CHANNEL.init();
            TCP_CHANNEL.startSource();
        }
    }

    protected static void startUDPServer() {
        if (updStart.compareAndSet(false, true)) {
            String value = ComponentCreator.getProperties().getProperty(UDP_PORT_PROPERTY_KEY);
            if (StringUtil.isNotEmpty(value)) {
                udpPort = Integer.valueOf(value);
            }
            UDP_CHANNEL.setProtocol(UDP);
            ;
            UDP_CHANNEL.setServerPort(udpPort + "");
            ;
            UDP_CHANNEL.init();
            UDP_CHANNEL.startSource();
        }
    }

}
