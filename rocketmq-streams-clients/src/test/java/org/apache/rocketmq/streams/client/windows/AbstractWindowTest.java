package org.apache.rocketmq.streams.client.windows;

import com.alibaba.fastjson.JSONObject;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.streams.client.transform.DataStream;
import org.apache.rocketmq.streams.client.transform.window.Time;
import org.apache.rocketmq.streams.client.transform.window.TumblingWindow;
import org.apache.rocketmq.streams.common.functions.ForEachFunction;
import org.apache.rocketmq.streams.common.functions.MapFunction;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;

public abstract class AbstractWindowTest implements Serializable {
    protected Long initEventTime=1627358460609L;
    protected String fileName="window_msg_";
    protected String filePath;

    public AbstractWindowTest(){
        String fileName=this.fileName+getSourceCount();
        filePath=SingleSplitTest.class.getClassLoader().getResource(fileName).getFile();
    }


    protected abstract int getSourceCount();

    /**
     *
     * @param isLocalOnly
     * @throws InterruptedException
     */
    public void testMutilWindow(boolean isLocalOnly)  {
        createWindowDataStream(isLocalOnly,5,0,100000,20L)
            .map(new MapFunction<JSONObject, JSONObject>() {
                long time=new Date().getTime();
                @Override
                public JSONObject map(JSONObject message) throws Exception {
                    message.put("name","chris");
                    message.put("time",time++);
                    return message;
                }
            })
            .window(TumblingWindow.of(Time.seconds(5)))
            .fireMode(2).waterMark(100000000)
            .setMaxMsgGap(80L)
            .groupBy("name")
            .setTimeField("time")
            .sum("total","sum_total")
            .setLocalStorageOnly(true)
            .toDataSteam()
            .forEach(new ForEachFunction<JSONObject>() {
                AtomicInteger sum = new AtomicInteger(0) ;
                Map<String,Integer> map=new HashMap<>();
                @Override
                public synchronized void foreach(JSONObject o) {
                    String windowInstanceId=o.getString("windowInstanceId");
                    Integer oldValue=map.get(windowInstanceId);
                    int total = o.getInteger("sum_total");
                    if(oldValue!=null){
                        total=total-oldValue;
                    }
                    int nowValue=sum.addAndGet(total);
                    map.put(windowInstanceId,total);
                    o.put("sum(total)",  nowValue);
                }
            }).toPrint().start();
    }

    /**
     * execute window data stream , need set parameters before execute
     * @param isLocalOnly  true: hign performace model ,not support exactly once
     * @param windowSize Tumbling window size
     * @param fireMode fire mode ,support three model
     * @param waterMark
     * @param maxMsgGap
     * @throws InterruptedException
     */
    protected void  executeWindowStream(boolean isLocalOnly,int windowSize, int fireMode, int waterMark,long maxMsgGap) throws InterruptedException {

       createWindowDataStream(isLocalOnly,windowSize,fireMode,waterMark,maxMsgGap).toFile("/tmp/rocketmq-streams/result.txt",true).toPrint().start();
    }


    protected DataStream createWindowDataStream(boolean isLocalOnly,int windowSize, int fireMode, int waterMark,long maxMsgGap){
        return createSourceDataStream().map(new MapFunction<JSONObject, JSONObject>() {
            Long time=null;
            @Override
            public JSONObject map(JSONObject message) throws Exception {

                //mode1,mode2 need source set logtime
                if(fireMode==0){
                    if(time==null){
                        time=initEventTime;
                    }else {
                        time++;
                    }
                    JSONObject msg=message;

                    msg.put("logTime",time);
                }


                return message;
            }
        })
            .window(TumblingWindow.of(Time.seconds(windowSize)))
            .groupBy("ProjectName", "LogStore")
            .setLocalStorageOnly(isLocalOnly)
            .setFireMode(fireMode)
            .setMaxMsgGap(maxMsgGap)
            .waterMark(waterMark)
            .setTimeField("logTime")
            .count("total")
            .sum("OutFlow", "OutFlow")
            .sum("InFlow", "inflow")
            .toDataSteam()
            .forEach(new ForEachFunction<JSONObject>() {
                AtomicInteger sum = new AtomicInteger(0) ;
                @Override
                public synchronized void foreach(JSONObject o) {
                    int total = o.getInteger("total");
                    o.put("sum_total",  sum.addAndGet(total));
                    o.put("currentTime", DateUtil.getCurrentTimeString());

                }
            });
    }


    /**
     *  validate the window result  meet expectations
     */
    protected void testWindowResult(int sourceCount){
        String path="/tmp/rocketmq-streams/result.txt";
        List<String> lines= FileUtil.loadFileLine(path);
        AtomicLong sum=new AtomicLong(0);//calculate result
        AtomicLong totalSum=new AtomicLong(0);
        Map<String,JSONObject> windowInstanceId2Msgs=new HashMap<>();
        //distinct by windowInstanceId，groupby(project,logstore),offset
        for(String line:lines) {
            JSONObject msg =null;
            try {
                 msg=JSONObject.parseObject(line);
            }catch (Exception e){
                e.printStackTrace();;
            }

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
            Long value=msg.getLong("total");
            totalSum.addAndGet(value);
            windowInstanceId2Msgs.put(key,msg);

        }

        /**
         * order by windowInstance,offset ase
         */
        List<JSONObject> msgList=new ArrayList<>(windowInstanceId2Msgs.values());
        Collections.sort(msgList, new Comparator<JSONObject>() {
            @Override public int compare(JSONObject o1, JSONObject o2) {
                String key1= MapKeyUtil.createKey(o1.getString("windowInstanceId"),o1.getLong("offset")+"");
                String key2= MapKeyUtil.createKey(o2.getString("windowInstanceId"),o2.getLong("offset")+"");
                return key1.compareTo(key2);
            }
        });

        //sum all count value
        Map<String,JSONObject> windowInstanceId2Offset=new HashMap<>();
        for(JSONObject msg:windowInstanceId2Msgs.values()){
            Long value=msg.getLong("total");
            sum.addAndGet(value);
        }
        assertTrue(sum.get()==sourceCount);
    }


    protected abstract DataStream createSourceDataStream();
}
