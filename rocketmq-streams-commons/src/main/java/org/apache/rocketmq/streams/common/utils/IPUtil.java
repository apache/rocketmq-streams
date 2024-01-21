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
package org.apache.rocketmq.streams.common.utils;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IPUtil {

    public static final String LOCALHOST = "127.0.0.1";
    public static final String ANYHOST = "0.0.0.0";
    private static final Logger LOG = LoggerFactory.getLogger(IPUtil.class);
    private static final Pattern IP_PATTERN = Pattern.compile("\\d{1,3}(\\.\\d{1,3}){3,5}$");
    private static volatile InetAddress LOCAL_ADDRESS = null;

    public static String getLocalIdentification() {
        return getLocalIP();
    }

    public static String getLocalAddress() {
        return getLocalIP();
    }

    public static String getLocalIP() {
        InetAddress address = getLocalIPAddress();
        return address == null ? LOCALHOST : address.getHostAddress();
    }

    public static InetAddress getLocalIPAddress() {
        if (LOCAL_ADDRESS != null) {
            return LOCAL_ADDRESS;
        }
        InetAddress localAddress = getLocalAddress0();
        LOCAL_ADDRESS = localAddress;
        return localAddress;
    }

    private static InetAddress getLocalAddress0() {
        InetAddress localAddress = null;
        try {
            localAddress = InetAddress.getLocalHost();
            if (isValidAddress(localAddress)) {
                return localAddress;
            }
        } catch (Throwable e) {
            LOG.warn("Failed to retrieving ip address, " + e.getMessage(), e);
        }
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            if (interfaces != null) {
                while (interfaces.hasMoreElements()) {
                    try {
                        NetworkInterface network = interfaces.nextElement();
                        Enumeration<InetAddress> addresses = network.getInetAddresses();
                        if (addresses != null) {
                            while (addresses.hasMoreElements()) {
                                try {
                                    InetAddress address = addresses.nextElement();
                                    if (isValidAddress(address)) {
                                        return address;
                                    }
                                } catch (Throwable e) {
                                    LOG.warn("Failed to retrieving ip address, " + e.getMessage(), e);
                                }
                            }
                        }
                    } catch (Throwable e) {
                        LOG.warn("Failed to retrieving ip address, " + e.getMessage(), e);
                    }
                }
            }
        } catch (Throwable e) {
            LOG.warn("Failed to retrieving ip address, " + e.getMessage(), e);
        }
        LOG.error("Could not get local host ip address, will use 127.0.0.1 instead.");
        return localAddress;
    }

    private static boolean isValidAddress(InetAddress address) {
        if (address == null || address.isLoopbackAddress()) {
            return false;
        }
        String name = address.getHostAddress();
        return (name != null && !ANYHOST.equals(name) && !LOCALHOST.equals(name) && IP_PATTERN.matcher(name).matches());
    }

    public static boolean isValidIp(String ip) {
        return (ip != null && !ANYHOST.equals(ip) && !LOCALHOST.equals(ip) && IP_PATTERN.matcher(ip).matches());
    }

    public static boolean isFuzzyQuery(String ip) {
        if (ip != null && ip.endsWith("*")) {
            return true;
        }
        return false;
    }

    public static String getFuzzyQueryIp(String ip) {
        if (ip != null && ip.length() > 1) {
            return ip.substring(0, ip.length() - 1);
        }
        return null;
    }

    /**
     * 域名格式检查
     *
     * @param domain
     * @return
     */
    public static boolean isDomainFormatCorrect(String domain) {
        if (null == domain) {
            return false;
        }
        String regx = "[a-zA-Z0-9][-a-zA-Z0-9]{0,62}(\\.[a-zA-Z0-9][-a-zA-Z0-9]{0,62})+\\.?";
        Pattern pattern = Pattern.compile(regx);
        Matcher matcher = pattern.matcher(domain);
        return matcher.matches();
    }

    /**
     * 判断一个ip是否在一个网段中
     *
     * @param ip   192.168.1.1
     * @param cidr 192.168.0.0/22
     * @return 是否在网段中
     */
    public static boolean isInRange(String ip, String cidr) {
        String[] ips = ip.split("\\.");
        int ipAddr = (Integer.parseInt(ips[0]) << 24)
            | (Integer.parseInt(ips[1]) << 16)
            | (Integer.parseInt(ips[2]) << 8)
            | Integer.parseInt(ips[3]);
        int type = Integer.parseInt(cidr.replaceAll(".*/", ""));
        int mask = 0xFFFFFFFF << (32 - type);
        String cidrIp = cidr.replaceAll("/.*", "");
        String[] cidrIps = cidrIp.split("\\.");
        int cidrIpAddr = (Integer.parseInt(cidrIps[0]) << 24)
            | (Integer.parseInt(cidrIps[1]) << 16)
            | (Integer.parseInt(cidrIps[2]) << 8)
            | Integer.parseInt(cidrIps[3]);

        return (ipAddr & mask) == (cidrIpAddr & mask);
    }

    /**
     * ip 是否在ip段中
     *
     * @param ip        "192.168.3.54"
     * @param ipSection 192.168.1.1-192.168.1.10
     * @return 存在返回true，否则返回false
     */
    public static boolean ipInSection(String ip, String ipSection) {
        if (ipSection == null) {
            throw new NullPointerException("IP段不能为空！");
        }
        if (ip == null) {
            throw new NullPointerException("IP不能为空！");
        }
        ipSection = ipSection.trim();
        ip = ip.trim();
        final String REGX_IP = "((25[0-5]|2[0-4]\\d|1\\d{2}|[1-9]\\d|\\d)\\.){3}(25[0-5]|2[0-4]\\d|1\\d{2}|[1-9]\\d|\\d)";
        final String REGX_IPB = REGX_IP + "\\-" + REGX_IP;
        if (!ipSection.matches(REGX_IPB) || !ip.matches(REGX_IP)) {
            return false;
        }
        int idx = ipSection.indexOf('-');
        String[] sips = ipSection.substring(0, idx).split("\\.");
        String[] sipe = ipSection.substring(idx + 1).split("\\.");
        String[] sipt = ip.split("\\.");
        long ips = 0L, ipe = 0L, ipt = 0L;
        for (int i = 0; i < 4; ++i) {
            ips = ips << 8 | Integer.parseInt(sips[i]);
            ipe = ipe << 8 | Integer.parseInt(sipe[i]);
            ipt = ipt << 8 | Integer.parseInt(sipt[i]);
        }
        if (ips > ipe) {
            long t = ips;
            ips = ipe;
            ipe = t;
        }
        return ips <= ipt && ipt <= ipe;
    }

}
