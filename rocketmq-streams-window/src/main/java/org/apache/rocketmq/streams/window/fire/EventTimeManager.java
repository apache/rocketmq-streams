package org.apache.rocketmq.streams.window.fire;

import java.util.HashMap;
import java.util.Map;

import org.apache.rocketmq.streams.common.channel.source.AbstractSource;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.configuable.ConfigurableComponent;
import org.apache.rocketmq.streams.window.operator.AbstractWindow;

public class EventTimeManager {
    private Map<String,SplitEventTimeManager> eventTimeManagerMap=new HashMap<>();
    protected ISource source;


    public void updateEventTime(IMessage message, AbstractWindow window){
        String queueId=message.getHeader().getQueueId();
        SplitEventTimeManager splitEventTimeManager=eventTimeManagerMap.get(queueId);
        if(splitEventTimeManager==null){
            synchronized (this){
                splitEventTimeManager=eventTimeManagerMap.get(queueId);
                if(splitEventTimeManager==null){
                    splitEventTimeManager=new SplitEventTimeManager(source,queueId);
                    eventTimeManagerMap.put(queueId,splitEventTimeManager);
                }
            }
        }
        splitEventTimeManager.updateEventTime(message,window);
    }



    public Long getMaxEventTime(String queueId){

        SplitEventTimeManager splitEventTimeManager=eventTimeManagerMap.get(queueId);
        if(splitEventTimeManager!=null){
            return splitEventTimeManager.getMaxEventTime();
        }
        return null;
    }

    public void setSource(ISource source) {
        if(this.source!=null){
            return;
        }
        synchronized (this){
            this.source = source;
            for(SplitEventTimeManager splitEventTimeManager:eventTimeManagerMap.values()){
                splitEventTimeManager.setSource(source);
            }
        }

    }
}
