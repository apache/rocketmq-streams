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
package org.apache.rocketmq.streams.window.debug;

import com.alibaba.fastjson.JSONObject;
import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.window.model.WindowCache;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.operator.AbstractWindow;

public class DebugWriter {
    protected String filePath="/tmp/rocksmq-streams/window_debug";
    protected static Map<String,DebugWriter> debugWriterMap=new HashMap<>();
    protected boolean openDebug=false;
    public DebugWriter(String windowName){
        filePath=filePath+"/"+windowName;
        File file=new File(filePath);
        file.deleteOnExit();
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


    public void writeWindowCache(AbstractWindow window, List<IMessage> messages,String splitId){
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



    public synchronized void writeShuffleCalculate(AbstractWindow window, List<IMessage> messages, WindowInstance instance){
        if(!openDebug){
            return;
        }
        String logFilePath=filePath+"/window_calculate/"+instance.getSplitId()+"/msg.txt";
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




    public synchronized void writeFireWindowInstance(WindowInstance windowInstance,Long eventTimeLastUpdateTime){
        if(!openDebug){
            return;
        }
        String logFilePath=filePath+"/window_calculate/"+windowInstance.getSplitId()+"/msg.txt";
        JSONObject fireMsg=new JSONObject();
        fireMsg.put("start_time",windowInstance.getStartTime());
        fireMsg.put("end_time",windowInstance.getEndTime());
        fireMsg.put("fire_time",windowInstance.getFireTime());
        fireMsg.put("queueid",windowInstance.getSplitId());
        fireMsg.put("lastUpdateTime", DateUtil.format(new Date(eventTimeLastUpdateTime)));
        fireMsg.put("sign","abc*********************************************abc");
        List<String> messages=new ArrayList<>();
        messages.add(fireMsg.toJSONString());
        FileUtil.write(logFilePath,messages,true);
    }

    public boolean isOpenDebug() {
        return openDebug;
    }
}
