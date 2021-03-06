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

package org.apache.rocketmq.streams.debug;

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.apache.rocketmq.streams.queue.RocketMQMessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DebugWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(DebugWriter.class);

    protected String dir = "/tmp/rocksmq-streams/mq";
    protected static Map<String, DebugWriter> debugWriterMap = new HashMap<>();

    public static DebugWriter getInstance(String topic) {
        DebugWriter debugWriter = debugWriterMap.get(topic);
        if (debugWriter == null) {
            debugWriter = new DebugWriter();
            debugWriterMap.put(topic, debugWriter);
        }
        return debugWriter;
    }

    public static boolean isOpenDebug() {
        return false;
    }

    public DebugWriter() {
    }

    public DebugWriter(String dir) {
        this.dir = dir;
    }

    /**
     * write offset 2 file
     *
     * @param offsets
     */
    public void writeSaveOffset(Map<MessageQueue, AtomicLong> offsets) {
        if (!isOpenDebug()) {
            return;
        }
        String path = dir + "/offsets/offset.txt";
        if (offsets == null || offsets.size() == 0) {
            return;
        }
        Iterator<Map.Entry<MessageQueue, AtomicLong>> it = offsets.entrySet().iterator();
        List<String> rows = new ArrayList<>();
        while (it.hasNext()) {
            Map.Entry<MessageQueue, AtomicLong> entry = it.next();
            String queueId = new RocketMQMessageQueue(entry.getKey()).getQueueId();
            JSONObject msg = new JSONObject();
            Long offset = entry.getValue().get();
            msg.put(queueId, offset);
            msg.put("saveTime", DateUtil.getCurrentTimeString());
            msg.put("queueId", queueId);
            rows.add(msg.toJSONString());
        }
        FileUtil.write(path, rows, true);
    }

    public void writeSaveOffset(MessageQueue messageQueue, AtomicLong offset) {
        if (!isOpenDebug()) {
            return;
        }
        Map<MessageQueue, AtomicLong> offsets = new HashMap<>();
        offsets.put(messageQueue, offset);
        writeSaveOffset(offsets);
    }

    public void receiveFirstData(String queueId, Long offset) {
        if (!isOpenDebug()) {
            return;
        }
        Map<String, Long> offsets = load();
        Long saveOffset = offsets.get(queueId);
        LOGGER.info("queueId is " + queueId + "current offset " + offset + "====" + saveOffset);
    }

    /**
     * load offsets
     *
     * @return
     */
    public Map<String, Long> load() {
        if (!isOpenDebug()) {
            return null;
        }
        String path = dir + "/offsets/offset.txt";
        List<String> lines = FileUtil.loadFileLine(path);
        Map<String, Long> offsets = new HashMap<>();
        for (String line : lines) {
            JSONObject row = JSONObject.parseObject(line);
            String queueId = row.getString("queueId");
            offsets.put(queueId, row.getLong(queueId));
        }
        return offsets;
    }
}
