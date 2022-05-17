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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.component.ComponentCreator;

public class TraceUtil {

    protected static final Log LOG = LogFactory.getLog(TraceUtil.class);

    public static final String TRACE_ID_FLAG = "traceId";

    private static final String RUNNING_MODE = "true";

    public static final String IGNORE_TRACE_ID = "-1";

    private static final String TRACE_SPLIT_FLAG = "@@";

    /**
     * the white list of trace id prefix, TODO load from database?
     */
    private static List<String> whiteSet = new ArrayList<String>() {
        {
            add("01010101-0000-1111-0101-0101010101");
            add("10101010-1111-0000-1010-1010101010");
        }
    };

    public static String increaseAndGet() {
        return UUID.randomUUID().toString();
    }

    public static void debug(String traceId, String... messages) {
        if (hit(traceId) && LOG.isDebugEnabled()) {
//            LOG.debug(envelope(traceId, messages));
        }
    }

    public static void info(String traceId, String... messages) {
        if (!skip(traceId) && LOG.isInfoEnabled()) {
//            LOG.info(envelope(traceId, messages));
        }
    }

    public static void warn(String traceId, String... messages) {
        if (LOG.isWarnEnabled()) {
//            LOG.warn(envelope(traceId, messages));
        }
    }

    public static void error(String traceId, String... messages) {
        if (LOG.isErrorEnabled()) {
//            LOG.error(envelope(traceId, messages));
        }
    }

    public static void error(String traceId, Throwable throwable, String... messages) {
        if (LOG.isErrorEnabled()) {
//            LOG.error(envelope(traceId, messages), throwable);
        }
    }

    private static boolean skip(String traceId) {
        if (IGNORE_TRACE_ID.equals(traceId)) {
            return true;
        }
        return false;
    }

    /**
     * 白名单及测试模式
     *
     * @param traceId
     * @return
     */
    public static boolean hit(String traceId) {
//        String type = ComponentCreator.getProperties().getProperty("dipper.trace.service.switch");
//        if (!RUNNING_MODE.equals(type)) {
//            return false;
//        }
//        for (String white : whiteSet) {
//            if (traceId.startsWith(white)) {
//                return true;
//            }
//        }
//
//        return false;
        return true;
    }

    private static String envelope(String traceId, String[] messages) {
        StringBuilder builder = new StringBuilder();
        builder.append(traceId).append(TRACE_SPLIT_FLAG);
        for (String message : messages) {
            builder.append(message).append(TRACE_SPLIT_FLAG);
        }
        return builder.toString();
    }

}
