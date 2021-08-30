package org.apache.rocketmq.streams.client.windows;

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.window.debug.WindowDebug;
import org.junit.Test;

public class WindowDebugTest {
    @Test
    public void windowDebug(){
        WindowDebug windowDebug=new WindowDebug("name1_window_10001","logTime","/tmp/rockstmq-streams","sum(total)",88121);
        windowDebug.startAnalysis();
    }

    /**
     *          .groupBy("ProjectName", "LogStore")
     */
    @Test
    public void testOutputResult(){
        String path="/tmp/rocketmq-streams/result.txt";
        List<String> lines= FileUtil.loadFileLine(path);
        AtomicLong sum=new AtomicLong(0);
        AtomicLong totalSum=new AtomicLong(0);
        Map<String,JSONObject> windowInstanceId2Msgs=new HashMap<>();
        Map<String,Integer>windowInstanceId2Count=new HashMap<>();
         int i=0;
        for(String line:lines) {
            JSONObject msg = JSONObject.parseObject(line);
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
            msg.put("index",i++);
            Long value=msg.getLong("total");
            totalSum.addAndGet(value);
            windowInstanceId2Msgs.put(key,msg);
            Integer count=windowInstanceId2Count.get(key);
            if(count==null){
                 count=0;
            }
            count++;
            windowInstanceId2Count.put(key,count);
        }

        int lessDateCount=0;
        for(String key:windowInstanceId2Count.keySet()){
            Integer count=windowInstanceId2Count.get(key);
            if(count!=2){
                lessDateCount++;
            }
        }
        System.out.println(lessDateCount);


        List<JSONObject> msgList=new ArrayList<>(windowInstanceId2Msgs.values());
        Collections.sort(msgList, new Comparator<JSONObject>() {
            @Override public int compare(JSONObject o1, JSONObject o2) {
                String key1= MapKeyUtil.createKey(o1.getString("windowInstanceId"),o1.getLong("offset")+"");
                String key2= MapKeyUtil.createKey(o2.getString("windowInstanceId"),o2.getLong("offset")+"");
                return key1.compareTo(key2);
            }
        });
        Map<String,JSONObject> windowInstanceId2Offset=new HashMap<>();
        for(JSONObject msg:windowInstanceId2Msgs.values()){
            Long currentOffset= msg.getLong("offset");
            String windowInstanceId=msg.getString("windowInstanceId");
            JSONObject old=windowInstanceId2Offset.get(windowInstanceId);
            Long value=msg.getLong("total");
            sum.addAndGet(value);
//            if(old==null){
//                windowInstanceId2Offset.put(windowInstanceId,msg);
//                sum.addAndGet(value);
//            }else {
//                if(currentOffset>old.getLong("offset")){
//                    windowInstanceId2Offset.put(windowInstanceId,msg);
//                    sum.addAndGet(value);
//                }else {
//                    System.out.println("   ");
//                }
//
//            }
        }
        System.out.println(sum.get()+"----"+totalSum.get());
    }





    @Test
    public void testMsgOrder(){
        String path="/tmp/rockstmq-streams/window_receive/1/msg.txt";
        List<String> lines=FileUtil.loadFileLine(path);
        int initOffset=0;
        for(String line:lines){
            JSONObject msg=JSONObject.parseObject(line);

            Integer offset=msg.getInteger("ori_offset");
            if(offset==null){
                continue;
            }
            initOffset++;
            if(offset!=initOffset){
                throw new RuntimeException("");
            }

        }
        System.out.println(initOffset);
    }

}
