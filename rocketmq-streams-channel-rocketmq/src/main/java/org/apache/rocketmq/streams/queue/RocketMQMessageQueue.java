package org.apache.rocketmq.streams.queue;

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;

public class RocketMQMessageQueue extends BasedConfigurable implements ISplit<RocketMQMessageQueue, MessageQueue> {
    protected transient MessageQueue queue;
    protected String brokeName;
    protected String topic;
    protected int mqQueueId;


    @Override
    protected void getJsonObject(JSONObject jsonObject) {
        super.getJsonObject(jsonObject);
        queue=new MessageQueue(topic,brokeName,mqQueueId);
    }


    public RocketMQMessageQueue(MessageQueue queue) {
        this.queue = queue;
        this.brokeName=queue.getBrokerName();
        this.topic=queue.getTopic();
        this.mqQueueId=queue.getQueueId();
    }

    public RocketMQMessageQueue() {

    }

    @Override
    public MessageQueue getQueue() {
        return queue;
    }

    @Override
    public int compareTo(RocketMQMessageQueue o) {
        return queue.compareTo(o.queue);
    }



    @Override
    public String getQueueId() {
        return getQueueId(this.queue);
    }

    @Override
    public String getPlusQueueId() {
        return MapKeyUtil.createKeyBySign("_",queue.getTopic(),queue.getBrokerName(),getSplitNumerStr(queue.getQueueId()+1)+"");
    }

    public static String getQueueId(MessageQueue queue){

        String[] topic = queue.getTopic().split("%");
        if (topic.length > 1) {
            return MapKeyUtil.createKeyBySign("_",topic[1],queue.getBrokerName(),getSplitNumerStr(queue.getQueueId())+"");
        }
        return MapKeyUtil.createKeyBySign("_",queue.getTopic(),queue.getBrokerName(),getSplitNumerStr(queue.getQueueId())+"");
    }
    /**
     * 获取分片的字符串格式，需要3位对齐
     * @param splitNumer
     * @return
     */
    private static String getSplitNumerStr(int splitNumer){
        int len=(splitNumer+"").length();
        if(len==3){
            return splitNumer+"";
        }
        String splitNumerStr=splitNumer+"";
        while (len<3){
            splitNumerStr="0"+splitNumerStr;
            len=splitNumerStr.length();
        }
        return splitNumerStr;
    }

    public String getBrokeName() {
        return brokeName;
    }

    public void setBrokeName(String brokeName) {
        this.brokeName = brokeName;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getMqQueueId() {
        return mqQueueId;
    }

    public void setMqQueueId(int mqQueueId) {
        this.mqQueueId = mqQueueId;
    }


}
