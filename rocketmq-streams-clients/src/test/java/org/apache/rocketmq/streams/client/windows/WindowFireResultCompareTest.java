package org.apache.rocketmq.streams.client.windows;

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.junit.Test;

public class WindowFireResultCompareTest {

    @Test
    public void testCompareResult(){
        List<String> first= FileUtil.loadFileLine("/tmp/rockstmq-streams/window_calculate/msg.txt");
        List<String> second= FileUtil.loadFileLine("/tmp/rockstmq-streams-1/window_calculate/msg.txt");
        Map<String,Integer>   instanceCounts=new HashMap<>();
        int firstTotal=0;
        Map<String, JSONObject> firstMsgs=new HashMap<>();
        for(int i=0;i<first.size();i++) {
            JSONObject jsonObject = JSONObject.parseObject(first.get(i));

            String msgKey = jsonObject.getString("msgKey");
            firstMsgs.put(msgKey,jsonObject);
        }
        List<JSONObject> firstMsgList=new ArrayList<>(firstMsgs.values());
        for(int i=0;i<firstMsgList.size();i++){
            JSONObject firstMsg=firstMsgList.get(i);

            String windowInstanceId = firstMsg.getString("windowInstanceId");
            String project = firstMsg.getString("ProjectName");
            String logstore = firstMsg.getString("LogStore");
            Long currentOffset= firstMsg.getLong("offset");
            Integer count=instanceCounts.get(windowInstanceId);
            if(count==null){
                count=0;
            }
            count++;
            instanceCounts.put(windowInstanceId,count);


        }

        for(Integer count:instanceCounts.values()){
            firstTotal+=count;
        }
        Map<String, JSONObject> secondMsgs=new HashMap<>();
        for(int i=0;i<second.size();i++) {
            JSONObject jsonObject = JSONObject.parseObject(second.get(i));

            String msgKey = jsonObject.getString("msgKey");
            secondMsgs.put(msgKey,jsonObject);
        }

        Map<String,Integer>   secondInstanceCounts=new HashMap<>();
        List<JSONObject> secondMsgList=new ArrayList<>(secondMsgs.values());
        for(int i=0;i<secondMsgList.size();i++) {
            JSONObject msg = secondMsgList.get(i);

            String windowInstanceId = msg.getString("windowInstanceId");
//            String project = firstMsg.getString("ProjectName");
//            String logstore = firstMsg.getString("LogStore");
//            Long currentOffset= firstMsg.getLong("offset");
            Integer count = secondInstanceCounts.get(windowInstanceId);
            if (count == null) {
                count = 0;
            }
            count++;
            secondInstanceCounts.put(windowInstanceId, count);


        }
        int secondTotal=0;
        for(Integer count:secondInstanceCounts.values()){
            secondTotal+=count;
        }
        System.out.println(instanceCounts.size()==secondInstanceCounts.size());
        System.out.println(firstTotal+" "+secondTotal);
        for(String windowInstanceId:instanceCounts.keySet()){
            Integer firstCount=instanceCounts.get(windowInstanceId);
            Integer secondCount=secondInstanceCounts.get(windowInstanceId);
            if(firstCount.longValue()!=secondCount.longValue()){
                System.out.println(windowInstanceId+"  "+firstCount+" "+secondCount);
            }
        }

    }

    /**
     *  msg.put("offset",message.getHeader().getOffset());
     *             msg.put("queueid",message.getMessageBody().getString(message.getHeader().getQueueId()));
     *             msg.put("windowInstaceId",instance.createWindowInstanceId());
     */
    @Test
    public void testCalculateMsgReceiver(){
        String filePath="/tmp/rocketmq-streams/result.txt";
        List<String> msgs=FileUtil.loadFileLine(filePath);
        Map<String,Integer> windowIntanceId2Count=new HashMap<>();
        for(String line:msgs){
            JSONObject msg=JSONObject.parseObject(line);
            String windowInstanceId = msg.getString("windowInstanceId");
            String project = msg.getString("ProjectName");
            String logstore = msg.getString("LogStore");
            Long currentOffset= msg.getLong("offset");
            if(StringUtil.isEmpty(project)){
                project="<null>";
            }
            if(StringUtil.isEmpty(logstore)){
                logstore="<null>";
            }
            String key = MapKeyUtil.createKey(windowInstanceId,project,logstore,currentOffset+"");
            Integer count=  windowIntanceId2Count.get(key);
            if(count==null){
                count=0;
            }
            count++;
            windowIntanceId2Count.put(key,count);
        }


        filePath="/tmp/rocketmq-streams/result.txt.1";
        msgs=FileUtil.loadFileLine(filePath);
        Map<String,Integer> windowIntanceId2Count_2=new HashMap<>();
        for(String line:msgs){
            JSONObject msg=JSONObject.parseObject(line);
            String windowInstanceId = msg.getString("windowInstanceId");
            String project = msg.getString("ProjectName");
            String logstore = msg.getString("LogStore");
            Long currentOffset= msg.getLong("offset");
            if(StringUtil.isEmpty(project)){
                project="<null>";
            }
            if(StringUtil.isEmpty(logstore)){
                logstore="<null>";
            }
            String key = MapKeyUtil.createKey(windowInstanceId,project,logstore,currentOffset+"");
            Integer count=  windowIntanceId2Count_2.get(key);
            if(count==null){
                count=0;
            }
            count++;
            windowIntanceId2Count_2.put(key,count);
        }

        System.out.println(windowIntanceId2Count.size()==windowIntanceId2Count_2.size());


        for(String key:windowIntanceId2Count.keySet()){
            Integer count=windowIntanceId2Count.get(key);
            Integer count2=windowIntanceId2Count_2.get(key);
            if(count.intValue()!=count2.intValue()){
                System.out.println(key+" "+count+"  "+count2);
            }
        }

    }

}
