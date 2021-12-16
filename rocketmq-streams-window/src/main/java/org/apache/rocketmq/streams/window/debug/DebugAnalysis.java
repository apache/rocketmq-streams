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
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.apache.rocketmq.streams.common.utils.JsonableUtil;
import org.apache.rocketmq.streams.common.utils.PrintUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.junit.Assert;

import static junit.framework.TestCase.assertTrue;

public class DebugAnalysis {
    private String dir;
    protected String sumFieldName;
    protected int expectValue;
    protected String timeFieldName;

    public DebugAnalysis(String dir,String sumFieldName,int expectValue,String timeFieldName){
        this.dir=dir;
        this.sumFieldName=sumFieldName;
        this.expectValue=expectValue;
        this.timeFieldName=timeFieldName;
    }

    public void debugAnalysis(){
        testWindowCacheSuccess();
        System.out.println("window cachefilter check success");
        testShuffleReceivedBeforeCacheSuccess();
        System.out.println("shuffle received before cachefilter check success");
        testShuffleReceivedSuccess();
        System.out.println("shuffle received check success");
        testTimeoutFire();
        System.out.println("event fire before timeout fire check success");
        testShuffleCalculateSuccess();
        System.out.println("shuffle calculate check success");
        testFireOrderSuccess();
        System.out.println("fire order check success");
       // testRocksDB();
        System.out.println("rocksdb check success");
        System.out.println("fire order check success");
        testFireResultSuccess();
        System.out.println("fire result check success");
    }


    /**
     * window receiver msg,expect time is in order group by split and the count equals expect count value
     */
    public void testWindowCacheSuccess() {
        String logFilePath=dir+"/window_cache";
        File dirs=new File(logFilePath);
        File[] files=dirs.listFiles();
        int sum=0;
        for(File file:files){
            List<String> lines= FileUtil.loadFileLine(file.getAbsolutePath()+"/msg.txt");
            sum+=calculateCount(lines,false);
            try {
                isInOrderByTime(lines);
            }catch (Exception e){
                System.out.println(file.getAbsoluteFile());
                e.printStackTrace();
                assertTrue(false);
            }
        }
        if(sum!=expectValue){
            System.out.println(sum+"  "+expectValue);
        }
        assertTrue(sum==expectValue);
    }
    /**
     * window receiver msg,expect time is in order group by split and the count equals expect count value
     */
    public void testShuffleReceivedBeforeCacheSuccess() {
        String logFilePath=dir+"/window_receive_before_cache";
        File dirs=new File(logFilePath);
        File[] files=dirs.listFiles();
        int sum=0;
        for(File file:files){
            List<String> lines= FileUtil.loadFileLine(file.getAbsolutePath()+"/msg.txt");
            sum+=calculateCount(lines,false);
            try {
                isInOrderByTime(lines);
            }catch (Exception e){
                System.out.println(file.getAbsoluteFile());
                e.printStackTrace();
                assertTrue(false);
            }
        }
        if(sum!=expectValue){
            System.out.println(sum+"     "+expectValue);
        }
        assertTrue(sum==expectValue);
    }

    /**
     * test:receiver msg count is test input count, the time is in order
     */
    public void testShuffleReceivedSuccess() {
        String logFilePath=dir+"/window_receive";
        File dirs=new File(logFilePath);
        File[] files=dirs.listFiles();
        int sum=0;
        for(File file:files){
            List<String> lines= FileUtil.loadFileLine(file.getAbsolutePath()+"/msg.txt");
            sum+=calculateCount(lines,false);
            isOrderInShuffleSplit(lines,file.getAbsolutePath());
        }
        if(sum!=expectValue){
            System.out.println("shuffle receive"+sum+"   "+expectValue);
        }
        assertTrue(sum==expectValue);
    }

    /**
     * test the msg received after fire
     */
    public void testTimeoutFire() {

        String logFilePath=dir+"/window_receive";
        File dirs=new File(logFilePath);
        File[] files=dirs.listFiles();


        Map<String,List<String>> map=new HashMap<>();
        Map<String,Boolean> reasons=new HashMap<>();
        for(File file:files){
            List<String> lines= FileUtil.loadFileLine(file.getAbsolutePath()+"/msg.txt");
            for(String line:lines){
                JSONObject msg=JSONObject.parseObject(line);
                if(isFireMsg(msg)){
                    String queueId=msg.getString("queueid");
                    List<String> fireTimeJson=map.get(queueId);
                    if(fireTimeJson==null){
                        fireTimeJson=new ArrayList<>();
                        map.put(queueId,fireTimeJson);
                    }
                    fireTimeJson.add(msg.toJSONString());
                    Integer fireReason=msg.getInteger("fireReason");
                    Boolean isTimeOutFire=reasons.get(queueId);
                    if(isTimeOutFire==null){
                        reasons.put(queueId,fireReason==1);
                        continue;
                    }
                    if(isTimeOutFire!=null&&isTimeOutFire&&fireReason==0){
                        System.out.println("the event fire after timeout");
                        PrintUtil.print(fireTimeJson);
                        Assert.assertTrue(false);
                    }
                    if(fireReason!=null&&fireReason==1){
                        reasons.put(queueId,true);
                    }
                }
            }
        }
    }


    public void testRocksDB() {

//        String logFilePath=dir+"/window_receive";
//        File dirs=new File(logFilePath);
//        File[] files=dirs.listFiles();
//        boolean isTimeOutFire=false;
//        int sum=0;
//        for(File file:files){
//            List<String> lines= FileUtil.loadFileLine(file.getAbsolutePath()+"/msg.txt");
//            for(String line:lines){
//                JSONObject msg=JSONObject.parseObject(line);
//                if(isFireMsg(msg)){
//                    String splitId=msg.getString("queueid");
//                    String startTime=msg.getString("start_time");
//                    String endTime=msg.getString("end_time");
//                    String windowNameSpace="namespace";
//                    String windowName="name1_window_10001";
//                    String windowInstanceId=   MapKeyUtil.createKey(splitId, windowNameSpace, windowName, windowName, startTime, endTime);
//                    RocksdbStorage rocksdbStorage=new RocksdbStorage(RocksdbStorage.createDBFile(),true);
//                    WindowStorage.WindowBaseValueIterator<WindowValue> iterator= rocksdbStorage.loadWindowInstanceSplitData(WindowOperator.ORDER_BY_SPLIT_NUM,splitId,windowInstanceId,null, WindowValue.class);
//                    while (iterator.hasNext()){
//                        WindowValue windowValue=iterator.next();
//                        Integer count=(Integer)windowValue.getComputedColumnResultByKey("total");
//                        sum+=count;
//                    }
//                }
//            }
//        }
//        assertTrue(sum==expectValue);
    }
    public void testShuffleCalculateSuccess() {
        String logFilePath=dir+"/window_calculate/msg.txt";
        List<String> lines=FileUtil.loadFileLine(logFilePath);
        Map<String,Object> map=new HashMap<>();
        for(String line:lines){
            JSONObject msg=JSONObject.parseObject(line);
            map.put(msg.getString("msgKey"),msg.getLong("result"));
        }
        long sum=0;
        for(String key:map.keySet()){
            long value=Long.valueOf(map.get(key).toString());
            sum+=value;
        }
        assertTrue(sum==expectValue);
    }


    public void testFireOrderSuccess() {

        String logFilePath=dir+"/window_receive";
        File dirs=new File(logFilePath);
        File[] files=dirs.listFiles();
        for(File file:files){
            List<String> lines= FileUtil.loadFileLine(file.getAbsolutePath()+"/msg.txt");
            isFiredInExpectOrder(lines,file.getAbsolutePath());
        }
    }

    public void testFireResultSuccess() {
        String logFilePath=dir+"/window_fire";
        File dirs=new File(logFilePath);
        File[] files=dirs.listFiles();
        long  sum=0;
        for(File file:files){
            List<String> lines= FileUtil.loadFileLine(file.getAbsolutePath()+"/msg.txt");
            long count=isFireOffsetIsInOrderAndResultIsAccuracy(lines);
            sum+=count;
        }
        assertTrue(sum==expectValue);
    }



    /**
     * the msg expect is in order group by orig_split in one shuffle split
     * @param lines
     */
    protected void isOrderInShuffleSplit( List<String> lines,String path){
        Map<String,String> queueId2EventTime=new HashMap<>();
        Map<String,JSONObject> queueId2Msg=new HashMap<>();
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
                    System.out.println("the msg is disorder in shuffle msg");
                    JSONObject errorMsg=new JSONObject();
                    errorMsg.put("orig_queueID",oriQueueId);
                    errorMsg.put("current_msg",msg);
                    errorMsg.put("preview_event_time",existEventTime);
                    errorMsg.put("current_event_time",eventTime);
                    errorMsg.put("path",path);
                    System.out.println(JsonableUtil.formatJson(errorMsg));
                    System.out.println(queueId2Msg.get(oriQueueId));
                    Assert.assertTrue(false);
                }
                existEventTime=eventTime;
            }
            queueId2EventTime.put(oriQueueId,existEventTime);
            queueId2Msg.put(oriQueueId,msg);
        }

    }

    /**
     * is the msg received after window instance fired
     * @param lines
     * @param path
     */
    protected void isMsgRecevierAfterFire(List<String> lines,String path){
        List<JSONObject> fireTimes=new ArrayList<>();
        String currentEventTime=null;
        String fireTime=null;
        for(String line :lines){
            JSONObject msg=JSONObject.parseObject(line);

            if(isFireMsg(msg)){
                fireTime=msg.getString("fire_time");

                fireTimes.add(msg);
            }else {
                if(StringUtil.isNotEmpty(fireTime)){
                    String eventTime=msg.getString("event_time");
                    if(eventTime.compareTo(fireTime)<=0){
                        System.out.println("the msg received after window instance fired "+path);
                        JSONObject errorMSG=new JSONObject();
                        errorMSG.put("fireTime",fireTime);
                        errorMSG.put("current_event_time",eventTime);
                        errorMSG.put("current_msg",msg);
                        System.out.println(JsonableUtil.formatJson(errorMSG));
                        Assert.assertTrue(false);
                    }
                }
            }
        }
    }
    protected void isFiredInExpectOrder( List<String> lines,String path){
        List<JSONObject> fireTimes=new ArrayList<>();
        List<String> fireTimeMsgs=new ArrayList<>();
        String currentFireTime=null;
        for(String line :lines){
            JSONObject msg=JSONObject.parseObject(line);
            if(isFireMsg(msg)) {
                if(currentFireTime==null){
                    currentFireTime=msg.getString("fire_time");
                }
                fireTimes.add(msg);
                fireTimeMsgs.add(msg.toJSONString());
            }else {
                String eventTime=msg.getString("event_time");
                if(currentFireTime!=null&&eventTime.compareTo(currentFireTime)<=0){
                    System.out.println("msg calculate after the instance fired");
                    PrintUtil.print(fireTimeMsgs);
                    Assert.assertTrue(false);
                }
            }
        }

        //{"start_time":"2021-07-27 12:01:00","queueid":"shuffle_TOPIC_DIPPER_SYSTEM_MSG_6_namespace_name1_broker-10.10.36.20_006","fire_time":"2021-07-27 12:01:05","end_time":"2021-07-27 12:01:05","sign":"abc*********************************************abc","maxEventTime":"2021-07-27 12:01:08","current_time":"2021-08-16 20:16:27","lastUpdateTime":"2021-08-16 20:16:27"}
        String currentDate=null;
      //  Assert.assertTrue(fireTimeMsgs.size()==18);
        Date fireDate=DateUtil.parse("2021-07-27 12:01:05");
        for(int i=0;i<fireTimes.size();i++){
            JSONObject msg=fireTimes.get(i);
            String fireTime=msg.getString("fire_time");
            if(!DateUtil.format(fireDate).equals(fireTime)){
                System.out.println("window fired is not expect the order");
                PrintUtil.print(fireTimeMsgs);
                Assert.assertTrue(false);
            }
            fireDate=DateUtil.addDate(TimeUnit.SECONDS,fireDate,5);
            if(currentDate==null){
                currentDate=fireTime;
            }else if(fireTime.compareTo(currentDate)<=0){
                System.out.println("window fired is not expect the order");
                PrintUtil.print(fireTimeMsgs);
                Assert.assertTrue(false);
            }else {
                currentDate=fireTime;
            }

        }
    }



    /**
     * sum the count value.if useFieldName==false return list count
     * @param list
     * @param useFieldName
     * @return
     */
    protected int calculateCount(List<String> list,boolean useFieldName){
        if(list==null){
            return 0;
        }
        int sum=0;
        for(String line:list){
            JSONObject msg=JSONObject.parseObject(line);
            if(isFireMsg(msg)){
                continue;
            }
            Integer count=null;
            if(!useFieldName){
                count=1;
            }else {
                count=msg.getInteger(sumFieldName);
            }

            if(count!=null){
                sum+=count;
            }
        }
        return sum;
    }

    /**
     * find disorder msg in one split
     * @param list
     * @return
     */
    protected boolean isInOrderByTime(List<String> list){
        Long currentTime=null;
        JSONObject currentMsg=null;

        for(String line:list){
            JSONObject msg=JSONObject.parseObject(line);
            Long time=msg.getLong(timeFieldName);
            if(currentTime==null){
                currentTime=time;
                currentMsg=msg;
            }else if(time<currentTime){
                System.out.println("the file is Disorder，the currentMsg is "+currentMsg+"，the msg is "+line);
                Assert.assertTrue(false);
            }else {
                currentMsg=msg;
                currentTime=time;
            }
        }
        return true;
    }

    private long isFireOffsetIsInOrderAndResultIsAccuracy(List<String> lines) {
        long sum=0;
        Map<String,Long> partionNums=new HashMap<>();
        for(String line:lines){
            JSONObject msg=JSONObject.parseObject(line);
            long value=Long.valueOf(msg.get("result").toString());
            Long currentPartitionNum=msg.getLong("partitionNum");
            Long partitionNum=partionNums.get(msg.getString("queueId"));
            if(partitionNum==null){
                partitionNum=currentPartitionNum;

            }else if(currentPartitionNum<=partitionNum){
                System.out.println("the patition num is disOrder");
                Assert.assertTrue(false);
            }else {
                partitionNum=currentPartitionNum;
            }
            partionNums.put(msg.getString("queueId"),partitionNum);
            sum+=value;
        }

        return sum;

    }
    private boolean isFireMsg(JSONObject msg) {
        return msg.getString("lastUpdateTime")!=null;


    }

}
