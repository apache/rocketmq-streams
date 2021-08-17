package org.apache.rocketmq.streams.window.debug;

import com.alibaba.fastjson.JSONObject;
import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.window.model.WindowCache;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.operator.AbstractWindow;
import org.apache.rocketmq.streams.window.state.WindowBaseValue;
import org.apache.rocketmq.streams.window.state.impl.WindowValue;

public class DebugWriter {
    protected String filePath="/tmp/rocketmq-streams/window_debug";
    protected static Map<String,DebugWriter> debugWriterMap=new HashMap<>();
    protected boolean openDebug=false;
    protected  String countFileName;
    protected boolean openRocksDBTest=false;
    public DebugWriter(String windowName){
        filePath=filePath+"/"+windowName;
        File file=new File(filePath);
        file.deleteOnExit();
        String value=ComponentCreator.getProperties().getProperty("window.debug");
        if(StringUtil.isNotEmpty(value)){
            openDebug=Boolean.valueOf(value);
        }
        value=ComponentCreator.getProperties().getProperty("window.debug.countFileName");
        if(StringUtil.isNotEmpty(value)){
            countFileName=value;
        }
        value=ComponentCreator.getProperties().getProperty("window.debug.dir");
        if(StringUtil.isNotEmpty(value)){
            filePath=value;
        }
        value=ComponentCreator.getProperties().getProperty("window.debug.rocksdb");
        if(StringUtil.isNotEmpty(value)){
            openRocksDBTest=Boolean.valueOf(value);
        }
    }

    public static DebugWriter getDebugWriter(String windowName){
        DebugWriter debugWriter=debugWriterMap.get(windowName);
        if(debugWriter!=null){
            return debugWriter;
        }
        synchronized (DebugWriter.class){
            debugWriter=new DebugWriter(windowName);
            debugWriterMap.put(windowName,debugWriter);
        }
        return debugWriter;
    }


    public synchronized void writeWindowCache(AbstractWindow window, List<IMessage> messages,String splitId){
        if(!openDebug){
            return;
        }
        String logFilePath=filePath+"/window_cache/"+splitId+"/msg.txt";
        List<String> msgs=new ArrayList<>();
        for(IMessage message:messages){
            JSONObject jsonObject=new JSONObject();
            jsonObject.put("currentTime", DateUtil.getCurrentTimeString());
            Long eventTime=message.getMessageBody().getLong(window.getTimeFieldName());
            if(eventTime!=null){
                jsonObject.put("event_time",DateUtil.format(new Date(eventTime)));
            }
            jsonObject.put("ori_queue_id",message.getHeader().getQueueId());
            jsonObject.put("ori_offset",message.getHeader().getOffset());
            msgs.add(jsonObject.toJSONString());
        }
        FileUtil.write(logFilePath,msgs,true);
    }

    public synchronized void writeShuffleReceiveBeforeCache(AbstractWindow window, List<IMessage> messages,String splitId){
        if(!openDebug){
            return;
        }
        String logFilePath=filePath+"/window_receive_before_cache/"+splitId+"/msg.txt";
        List<String> msgs=new ArrayList<>();
        for(IMessage message:messages){
            JSONObject jsonObject=new JSONObject();
            jsonObject.put("currentTime", DateUtil.getCurrentTimeString());
            Long eventTime=message.getMessageBody().getLong(window.getTimeFieldName());
            if(eventTime!=null){
                jsonObject.put("event_time",DateUtil.format(new Date(eventTime)));
            }
            jsonObject.put("ori_queue_id",message.getHeader().getQueueId());
            jsonObject.put("ori_offset",message.getHeader().getOffset());
            msgs.add(jsonObject.toJSONString());
        }
        FileUtil.write(logFilePath,msgs,true);
    }


    public synchronized void writeShuffleReceive(AbstractWindow window, List<IMessage> messages, WindowInstance instance){
        if(!openDebug){
            return;
        }
        String logFilePath=filePath+"/window_receive/"+instance.getSplitId()+"/msg.txt";
        List<String> msgs=new ArrayList<>();
        for(IMessage message:messages){
            JSONObject jsonObject=new JSONObject();
            //jsonObject.put("logTime",message.getMessageBody().getString("logTime"));
            if(instance!=null){
                jsonObject.put("start_time",instance.getStartTime());
                jsonObject.put("end_time",instance.getEndTime());
                jsonObject.put("fire_time",instance.getFireTime());

            }
            jsonObject.put("currentTime", DateUtil.getCurrentTimeString());
            Long maxEventTime=window.getMaxEventTime(instance.getSplitId());
            if(maxEventTime!=null){
                jsonObject.put("max_event_time",DateUtil.format(new Date(maxEventTime)));
            }

            Long eventTime=message.getMessageBody().getLong(window.getTimeFieldName());
            if(eventTime!=null){
                jsonObject.put("event_time",DateUtil.format(new Date(eventTime)));
            }
            String lastUpdateTime=message.getMessageBody().getString("lastUpdateTime");
            if(StringUtil.isNotEmpty(lastUpdateTime)){
                jsonObject.put("lastUpdateTime",lastUpdateTime);
            }

            String oriQueueId = message.getMessageBody().getString(WindowCache.ORIGIN_QUEUE_ID);
            if(StringUtil.isNotEmpty(oriQueueId)){
                jsonObject.put("ori_queue_id",oriQueueId);
                jsonObject.put("ori_offset",message.getMessageBody().getString(WindowCache.ORIGIN_OFFSET));
            }

            msgs.add(jsonObject.toJSONString());
        }
        FileUtil.write(logFilePath,msgs,true);
    }

    public void writeWindowCalculate(AbstractWindow window, List<WindowValue> messages,String splitId){
        if(!openDebug){
            return;
        }
        String logFilePath=filePath+"/window_calculate/msg.txt";
        List<String> msgs=new ArrayList<>();
        for(WindowValue windowValue:messages){
            JSONObject jsonObject=new JSONObject();
            jsonObject.put("msgKey", windowValue.getMsgKey());
            jsonObject.put("result",windowValue.getComputedColumnResultByKey(countFileName).toString());
            msgs.add(jsonObject.toJSONString());
        }
        FileUtil.write(logFilePath,msgs,true);
    }

    public void writeWindowFire(AbstractWindow window, List<IMessage> messages,String splitId){
        if(!openDebug){
            return;
        }
        String logFilePath=filePath+"/window_fire/"+splitId+"/msg.txt";
        List<String> msgs=new ArrayList<>();
        for(IMessage msg:messages){
            JSONObject jsonObject=new JSONObject();
            jsonObject.put("partitionNum", msg.getHeader().getOffset());
            jsonObject.put("result",msg.getMessageBody().getLong("total"));
            jsonObject.put("queueId",msg.getHeader().getQueueId());
            msgs.add(jsonObject.toJSONString());
        }
        FileUtil.write(logFilePath,msgs,true);
    }


    public synchronized void writeFireWindowInstance(WindowInstance windowInstance,Long eventTimeLastUpdateTime,Long maxEventTime,int fireReason){
        if(!openDebug){
            return;
        }
        String logFilePath=filePath+"/window_receive/"+windowInstance.getSplitId()+"/msg.txt";
        JSONObject fireMsg=new JSONObject();
        fireMsg.put("current_time",DateUtil.getCurrentTimeString());
        fireMsg.put("start_time",windowInstance.getStartTime());
        fireMsg.put("end_time",windowInstance.getEndTime());
        fireMsg.put("fire_time",windowInstance.getFireTime());
        fireMsg.put("queueid",windowInstance.getSplitId());
        fireMsg.put("fireReason",fireReason);
        fireMsg.put("lastUpdateTime", DateUtil.format(new Date(eventTimeLastUpdateTime)));
        if(maxEventTime!=null){
            fireMsg.put("maxEventTime", DateUtil.format(new Date(maxEventTime)));
        }

        fireMsg.put("sign","abc*********************************************abc");
        List<String> messages=new ArrayList<>();
        messages.add(fireMsg.toJSONString());
        FileUtil.write(logFilePath,messages,true);
    }

    public boolean isOpenDebug() {
        return openDebug;
    }

    public String getCountFileName() {
        return countFileName;
    }

    public void setCountFileName(String countFileName) {
        this.countFileName = countFileName;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public void setOpenDebug(boolean openDebug) {
        this.openDebug = openDebug;
    }

    public boolean isOpenRocksDBTest() {
        return openRocksDBTest;
    }
}
