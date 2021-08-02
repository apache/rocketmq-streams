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

import java.io.File;
import java.lang.management.ManagementFactory;

/**
 * get current runtime information
 */
public class RuntimeUtil {

    public static String getPid() {
        try {
            String information = ManagementFactory.getRuntimeMXBean().getName();
            return information.split("@")[0];
        } catch (Exception e) {
            return null;
        }
    }

    public static String getUserDir() {
        try {
            String userDir = System.getProperty("user.dir");
            if (userDir != null) {
                return userDir.replace("/", "-").substring(userDir.length() - 10);
            }
        } catch (Exception e) {
            return null;
        }
        return null;
    }

    /**
     * 获取dipper 唯一标识
     *
     * @return
     */
    public static String getDipperInstanceId() {

        return MapKeyUtil.createKeyBySign("_", IPUtil.getLocalIdentification(), getPid()).replaceAll("\\.", "_");

    }

    public static void main(String[] args) {
        File file = new File("/tmp/" + getDipperInstanceId());
        file.mkdirs();
    }
}
