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
package org.apache.rocketmq.streams.filter.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IPUtil {

    private static final Log LOG = LogFactory.getLog(IPUtil.class);

    private static volatile InetAddress LOCAL_ADDRESS = null;
    private static final Pattern IP_PATTERN = Pattern.compile("\\d{1,3}(\\.\\d{1,3}){3,5}$");

    public static final String LOCALHOST = "127.0.0.1";
    public static final String ANYHOST = "0.0.0.0";
    private static final Pattern pattern = Pattern.compile(
        "^(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|[1-9])" + "\\.(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)"
            + "\\.(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)" + "\\.(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)$");
    private static final long ALLONES = (long)(Math.pow(2, 32) - 1);

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
     * 检查IP格式
     *
     * @param ip
     * @return
     */
    public static boolean checkIpFormat(String ip) {
        if (null == ip) {
            return false;
        }

        Matcher matcher = pattern.matcher(ip);
        return matcher.matches();
    }

    public static String ipToString(long ipInt) {
        String[] ipArray = new String[4];
        for (int i = 0; i < 4; i++) {
            ipArray[i] = String.valueOf(String.valueOf(ipInt & 0xFF));
            ipInt >>= 8;
        }
        return ipArray[3] + "." + ipArray[2] + "." + ipArray[1] + "." + ipArray[0];
    }

    public static long ipToInt(String ip) {
        String[] ips = ip.split("\\.");
        long packedIp = 0;
        for (String ipstr : ips) {
            packedIp = (packedIp << 8 | Integer.parseInt(ipstr));
        }
        return packedIp;
    }

    public static long getPrefixIp(int mask) {
        return (ALLONES ^ (ALLONES >> mask));
    }

    public static long getSuffixIp(int mask) {
        return ALLONES >> mask;
    }

    public static boolean isIpSegment(String ip) {
        if (checkIpSegFormat(ip)) {
            return true;
        }
        return false;
    }

    public static boolean checkIpSegFormat(String ip) {
        if (null == ip || "".equals(ip)) {
            return false;
        }

        int n = ip.indexOf("/");
        if (n <= 0) {
            return false;
        }

        String preIp = ip.substring(0, n);
        int mask = Integer.parseInt(ip.substring(n + 1));

        return checkMask(mask) && checkIpFormat(preIp);
    }

    public static boolean checkMask(int mask) {
        if (mask >= 0 && mask <= 32) {
            return true;
        }
        return false;
    }
}
