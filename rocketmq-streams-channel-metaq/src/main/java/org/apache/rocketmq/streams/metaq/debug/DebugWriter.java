package org.apache.rocketmq.streams.metaq.debug;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.rocketmq.common.message.MessageQueue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.apache.rocketmq.streams.metaq.queue.MetaqMessageQueue;

public class DebugWriter {

    protected static Map<String, DebugWriter> debugWriterMap = new HashMap<>();
    protected String dir = "/tmp/rocketmq-streams/mq";
    protected String topic;

    public DebugWriter(String topic) {
        this.topic = topic;
        this.dir = this.dir + "/" + topic.replaceAll("\\.", "_");
    }

    public DebugWriter(String topic, String dir) {
        this.topic = topic;
        this.dir = dir;
    }

    public static DebugWriter getInstance(String topic) {
        DebugWriter debugWriter = debugWriterMap.get(topic);
        if (debugWriter == null) {
            debugWriter = new DebugWriter(topic);
            debugWriterMap.put(topic, debugWriter);
        }
        return debugWriter;
    }

    public static boolean isOpenDebug() {
        return ComponentCreator.getPropertyBooleanValue("metq.debug.switch");
    }

    /**
     * write offset 2 file
     *
     * @param offsets
     */
    public void writeSaveOffset(Map<MessageQueue, AtomicLong> offsets) {
        String path = dir + "/offsets/offset.txt";
        if (offsets == null || offsets.size() == 0) {
            return;
        }
        Iterator<Map.Entry<MessageQueue, AtomicLong>> it = offsets.entrySet().iterator();
        List<String> rows = new ArrayList<>();
        while (it.hasNext()) {
            Map.Entry<MessageQueue, AtomicLong> entry = it.next();
            String queueId = new MetaqMessageQueue(entry.getKey()).getQueueId();
            if (queueId.indexOf("%") != -1) {
                continue;
            }
            JSONObject msg = new JSONObject();
            Long offset = entry.getValue().get();
            msg.put(queueId, offset);
            msg.put("saveTime", DateUtil.getCurrentTimeString());
            msg.put("queueId", queueId);
            msg.put("saveCheckPoint", true);
            rows.add(msg.toJSONString());
        }
        FileUtil.write(path, rows, true);
    }

    public void writeSaveOffset(MessageQueue messageQueue, AtomicLong offset) {
        Map<MessageQueue, AtomicLong> offsets = new HashMap<>();
        offsets.put(messageQueue, offset);
        writeSaveOffset(offsets);
    }

    public void receiveFirstData(String queueId, Long offset) {
        String path = dir + "/offsets/offset.txt";
        Map<String, Long> offsets = load();
        Long saveOffset = offsets.get(queueId);
        JSONObject msg = new JSONObject();
        msg.put(queueId, offset);
        msg.put("receiver", true);
        msg.put("save_offset", saveOffset);
        msg.put("current_time", DateUtil.getCurrentTimeString());
        List<String> rows = new ArrayList<>();
        rows.add(msg.toJSONString());
        FileUtil.write(path, rows, true);
        System.out.println("queueId is " + queueId + "current offset " + offset + "====" + saveOffset);
    }

    /**
     * load offsets
     *
     * @return
     */
    public Map<String, Long> load() {
        String path = dir + "/offsets/offset.txt";
        List<String> lines = FileUtil.loadFileLine(path);
        Map<String, Long> offsets = new HashMap<>();
        for (String line : lines) {
            JSONObject row = JSONObject.parseObject(line);
            if (row.getBoolean("saveCheckPoint") == null || !row.getBoolean("saveCheckPoint")) {
                continue;
            }
            String queueId = row.getString("queueId");
            offsets.put(queueId, row.getLong(queueId));
        }
        return offsets;
    }
}
