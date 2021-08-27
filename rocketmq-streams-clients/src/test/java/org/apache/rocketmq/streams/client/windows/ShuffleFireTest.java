package org.apache.rocketmq.streams.client.windows;

import com.alibaba.fastjson.JSONObject;
import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.junit.Assert;
import org.junit.Test;

public class ShuffleFireTest {

    @Test
    public void testShuffleFire(){
        String dir="/tmp/dipper/window_test";
        File file=new File(dir);
       File[] files= file.listFiles();
       for(File file1:files){
           doFile(file1.getAbsoluteFile()+"/msgs.txt");
       }
    }


    public void testFlink(ChainPipeline pipeline){
      
    }


    @Test
    public void testOrderFile(){
        String dir="/tmp/dipper/window_test";
        File file=new File(dir);
        File[] files= file.listFiles();
        for(File file1:files){
            doOrderFile(file1.getAbsoluteFile()+"/msgs.txt");
        }
    }


    private void doOrderFile(String path) {
        List<String> lines= FileUtil.loadFileLine(path);
        Map<String,String>  queueId2EventTime=new HashMap<>();
        for(String line :lines){
            JSONObject msg=JSONObject.parseObject(line);
            if(isFireMsg(msg)){
                continue;
            }
            String oriQueueId=msg.getString("ori_queue_id");
            String  existEventTime=queueId2EventTime.get(oriQueueId);
            String eventTime=msg.getString("event_time");
            if(existEventTime==null){
                existEventTime=eventTime;
            }else {
                if(existEventTime.compareTo(eventTime)>0){
                    System.out.println(existEventTime+"  "+eventTime+"  "+"  "+oriQueueId+" "+msg);
                    System.out.println(path);
                    Assert.assertTrue(false);
                }
                existEventTime=eventTime;
            }
            queueId2EventTime.put(oriQueueId,existEventTime);
        }
    }

    protected void doFile(String path){
        List<String> lines= FileUtil.loadFileLine(path);
        List<JSONObject> fireTimes=new ArrayList<>();
        String currentEventTime=null;
        String fireTime=null;
        String maxEventTime=null;
        for(String line :lines){
            JSONObject msg=JSONObject.parseObject(line);

            if(isFireMsg(msg)){
                fireTime=msg.getString("fire_time");

                fireTimes.add(msg);
            }else {
                if(StringUtil.isNotEmpty(fireTime)){
                    String eventTime=msg.getString("event_time");
                    if(eventTime.compareTo(fireTime)<=0){
                        System.out.println(eventTime+"  "+fireTime+" "+path);
                       //Assert.assertTrue(false);
                    }
                }
                String max_event_time=msg.getString("max_event_time");
                if(currentEventTime==null){
                    currentEventTime=max_event_time;
                }else if(max_event_time.compareTo(currentEventTime)>=0){
                    currentEventTime=max_event_time;
                }else {
                    System.out.println(path+" "+"  "+currentEventTime+"  "+max_event_time+"  "+msg.toString());
                }
            }
        }
     // Assert.assertTrue(fireTimes.size()==18);
        System.out.println("fire times is "+fireTimes.size());
        String start="2021-07-27 12:01:05";
        //String end="2021-07-27 12:02:30";
        Date startDate= DateUtil.parse(start);
        Date currentDate=startDate;
        for(int i=0;i<fireTimes.size();i++){
           JSONObject msg=fireTimes.get(i);
           String endTime=msg.getString("fire_time");
           if(!endTime.equals(DateUtil.format(currentDate))){
               System.out.println("========"+endTime+"  "+ DateUtil.format(currentDate)+" "+msg);
               Assert.assertTrue(false);
           }
           currentDate= DateUtil.addDate(TimeUnit.SECONDS,currentDate,5);

        }
    }

    private boolean isFireMsg(JSONObject msg) {
       return msg.getString("lastUpdateTime")!=null;


    }



    @Test
    public void testCheckMaxEventTime(){
        List<String> lines= FileUtil.loadFileLine("/tmp/dipper/window_test/shuffle_TOPIC_DIPPER_SYSTEM_MSG_6_namespace_name1_broker-10.10.40.15_006/msgs.txt");
        Set<String>  queueids=new HashSet<>();
        for(String line :lines){
            JSONObject msg=JSONObject.parseObject(line);
            String max_event_time=msg.getString("max_event_time");
            if(StringUtil.isEmpty(max_event_time)){
                getOrigQueueIds(msg,queueids);
            }
            else if(msg.getString("ori_queue_id")!=null&&!queueids.contains(msg.getString("ori_queue_id"))) {
                getOrigQueueIds(msg,queueids);
            }
        }
        System.out.println(queueids.size());
        //assertTrue((queueids.size()+1)==20);
    }



    protected String getOrigQueueIds( JSONObject msg,Set<String>  queueids){
        String origQueueid=msg.getString("ori_queue_id");
        if(origQueueid!=null){
            queueids.add(origQueueid);
        }
        return origQueueid;
    }
}
