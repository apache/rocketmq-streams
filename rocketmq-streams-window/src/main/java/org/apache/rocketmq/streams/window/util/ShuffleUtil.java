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
package org.apache.rocketmq.streams.window.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.topology.shuffle.IShuffleKeyGenerator;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.window.model.WindowCache;

public class ShuffleUtil {
    private static final Log LOG = LogFactory.getLog(ShuffleUtil.class);
    public static IMessage createShuffleMsg(IMessage msg, String shuffleKey,JSONObject msgHeader){
        if (msg.getHeader().isSystemMessage()) {
            return null;
        }

        if (StringUtil.isEmpty(shuffleKey)) {
            shuffleKey = "<null>";
            LOG.debug("there is no group by value in message! " + msg.getMessageBody().toString());
            //continue;
        }

        JSONObject body = msg.getMessageBody();
        String offset = msg.getHeader().getOffset();
        String queueId = msg.getHeader().getQueueId();

        body.put(WindowCache.ORIGIN_OFFSET, offset);
        body.put(WindowCache.ORIGIN_QUEUE_ID,queueId);
        body.put(WindowCache.ORIGIN_QUEUE_IS_LONG, msg.getHeader().getMessageOffset().isLongOfMainOffset());
        if(msgHeader==null){
            body.put(WindowCache.ORIGIN_MESSAGE_HEADER, JSONObject.toJSONString(msg.getHeader()));
        }else {
            body.put(WindowCache.ORIGIN_MESSAGE_HEADER, msgHeader);
        }

        body.put(WindowCache.ORIGIN_MESSAGE_TRACE_ID, msg.getHeader().getTraceId());
        body.put(WindowCache.SHUFFLE_KEY, shuffleKey);
        return msg;
    }

    public static IMessage createShuffleMsg(IMessage msg, String shuffleKey){
        return createShuffleMsg(msg,shuffleKey,null);
    }
}
