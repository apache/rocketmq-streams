package org.apache.rocketmq.streams.window.shuffle;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.db.driver.orm.ORMUtil;
import org.apache.rocketmq.streams.window.debug.DebugWriter;
import org.apache.rocketmq.streams.window.model.WindowCache;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.offset.WindowMaxValue;
import org.apache.rocketmq.streams.window.operator.AbstractShuffleWindow;
import org.apache.rocketmq.streams.window.operator.AbstractWindow;
import org.apache.rocketmq.streams.window.sqlcache.impl.SQLElement;
import org.apache.rocketmq.streams.window.sqlcache.impl.SplitSQLElement;
import org.apache.rocketmq.streams.window.state.WindowBaseValue;

/**
 *
 * save receiver messages into cachefilter
 * when checkpoint/autoflush/flush， process cachefilter message
 *
 * */
public class ShuffleCache extends WindowCache {
    protected AbstractShuffleWindow window;

    public ShuffleCache(AbstractShuffleWindow window) {
        this.window=window;
    }
    protected transient AtomicInteger COUNT=new AtomicInteger(0);
    @Override
    protected boolean batchInsert(List<IMessage> messageList) {
//            List<WindowInstance> windowInstances = (List<WindowInstance>)messageList.get(0).getMessageBody().get(WindowInstance.class.getSimpleName());
//            DebugWriter.getDebugWriter(window.getConfigureName()).writeShuffleReceive(window,messageList,windowInstances.get(0));
        Map<Pair<String, String>, List<IMessage>> instance2Messages = new HashMap<>();
        Map<String, WindowInstance> windowInstanceMap = new HashMap<>();
        groupByWindowInstanceAndQueueId(messageList, instance2Messages, windowInstanceMap);
        List<Pair<String, String>> keys=new ArrayList<>(instance2Messages.keySet());
        Collections.sort(keys);
        for(Pair<String, String> queueIdAndInstanceKey:keys){
            String queueId=queueIdAndInstanceKey.getLeft();
            String windowInstanceId=queueIdAndInstanceKey.getRight();
            List<IMessage> messages = instance2Messages.get(queueIdAndInstanceKey);
            WindowInstance windowInstance = windowInstanceMap.get(windowInstanceId);
            DebugWriter.getDebugWriter(window.getConfigureName()).writeShuffleReceive(window,messages,windowInstance);
            int count=COUNT.addAndGet(messages.size());
//            if(count>25000){
//                System.out.println("shuffle cal is "+count);
//            }
            window.shuffleCalculate(messages, windowInstance, queueId);
            saveSplitProgress(queueId,messages);
        }
        return true;
    }


    /**
     * save consumer progress（offset）for groupby  source queueId
     * @param queueId
     * @param messages
     */
    protected void saveSplitProgress(String queueId, List<IMessage> messages) {
        Map<String,String> queueId2OrigOffset=new HashMap<>();
        Set<String> oriQueueIds=new HashSet<>();
        Boolean isLong=false;
        for(IMessage message:messages){
            isLong=message.getMessageBody().getBoolean(ORIGIN_QUEUE_IS_LONG);
            String oriQueueId = message.getMessageBody().getString(WindowCache.ORIGIN_QUEUE_ID);
            String oriOffset = message.getMessageBody().getString(WindowCache.ORIGIN_OFFSET);
            queueId2OrigOffset.put(oriQueueId,oriOffset);
            oriQueueIds.add(oriQueueId);
        }
        Map<String,WindowMaxValue> windowMaxValueMap=window.getWindowMaxValueManager().saveMaxOffset(isLong,window.getConfigureName(),queueId,queueId2OrigOffset);
        window.getSqlCache().addCache(new SplitSQLElement(queueId,ORMUtil.createBatchReplacetSQL(new ArrayList<>(windowMaxValueMap.values()))));

    }


    @Override
    protected String generateShuffleKey(IMessage message) {
        return null;
    }


    /**
     * 根据message，把message分组到不同的group，分别处理
     *
     * @param messageList
     * @param instance2Messages
     * @param windowInstanceMap
     */
    protected void groupByWindowInstanceAndQueueId(List<IMessage> messageList, Map<Pair<String, String>, List<IMessage>> instance2Messages,
        Map<String, WindowInstance> windowInstanceMap) {
        for (IMessage message : messageList) {

            List<WindowInstance> windowInstances = (List<WindowInstance>)message.getMessageBody().get(WindowInstance.class.getSimpleName());
            String queueId = message.getHeader().getQueueId();
            for(WindowInstance windowInstance:windowInstances){
                String windowInstanceId = windowInstance.createWindowInstanceId();
                Pair<String, String> queueIdAndInstanceKey = Pair.of(queueId, windowInstanceId);
                List<IMessage> messages = instance2Messages.get(queueIdAndInstanceKey);
                if (messages == null) {
                    messages = new ArrayList<>();
                    instance2Messages.put(queueIdAndInstanceKey, messages);
                }
                messages.add(message);
                windowInstanceMap.put(windowInstanceId, windowInstance);
            }

            String oriQueueId = message.getMessageBody().getString(WindowCache.ORIGIN_QUEUE_ID);
            String oriOffset = message.getMessageBody().getString(WindowCache.ORIGIN_OFFSET);
            Boolean isLong = message.getMessageBody().getBoolean(WindowCache.ORIGIN_QUEUE_IS_LONG);
            message.getHeader().setQueueId(oriQueueId);
            message.getHeader().setOffset(oriOffset);
            message.getHeader().setOffsetIsLong(isLong);

        }
    }

//
//    public synchronized void addNeedFlushWindowInstance(WindowInstance windowInstance){
//        if(!window.isLocalStorageOnly()){
//            this.needSaveWindowInstances.add(windowInstance);
//        }
//    }
//
//    public synchronized void clearCache(WindowInstance windowInstance){
//        this.needSaveWindowInstances.remove(windowInstance);
//    }
}
