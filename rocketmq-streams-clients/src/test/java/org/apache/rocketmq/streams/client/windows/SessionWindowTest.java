package org.apache.rocketmq.streams.client.windows;

import java.io.Serializable;

public class SessionWindowTest implements Serializable {



    //@Test
    //public void testWindowFromFile() throws InterruptedException {
    //    MemoryCache memoryCache=new MemoryCache();
    //    AtomicInteger sum = new AtomicInteger(0) ;
    //   ConfiguableDataStream dataStream= StreamBuilder.dataStream("namespace", "name1")
    //        .fromMemory(memoryCache,true)
    //        .window(SessionWindow.of(Time.seconds(5)))
    //        .groupby("ProjectName", "LogStore")
    //        .setTimeField("logTime")
    //        .setLocalStorageOnly(true)
    //       // .setMaxMsgGap(1L)
    //        .count("total")
    //        .sum("OutFlow", "OutFlow")
    //        .sum("InFlow", "inflow")
    //        .toDataSteam()
    //        .forEach(new ForEachFunction<JSONObject>() {
    //
    //
    //            @Override
    //            public synchronized void foreach(JSONObject o) {
    //                int total = o.getInteger("total");
    //                o.put("sum(total)",  sum.addAndGet(total));
    //            }
    //        }).toPrint().asynStart();
    //
    //    MemorySource memorySource=(MemorySource)dataStream.getSource();
    //    Date date=new Date();
    //    date.setYear(2021-1900);
    //    date.setMonth(6);
    //    date.setDate(14);
    //    date.setHours(12);
    //    date.setMinutes(10);
    //    date.setSeconds(0);
    //    for(int i=0;i<3;i++){
    //        JSONObject msg=new JSONObject();
    //        msg.put("ProjectName","chris_project"+(i%10));
    //        msg.put("LogStore","chris_logstore");
    //        msg.put("logTime",date.getTime()+i*3000);
    //        msg.put("logTimeFormat", DateUtil.format(new Date(msg.getLong("logTime"))));
    //        msg.put("OutFlow",i);
    //        msg.put("InFlow",i);
    //        memorySource.getMemoryCache().addMessage(msg);
    //    }
    //    //memorySource.sendCheckpoint(memorySource.getQueueId());
    //
    //
    //    JSONObject msg=new JSONObject();
    //    msg.put("ProjectName","chris1_project");
    //    msg.put("LogStore","chris_logstore");
    //    msg.put("logTime",date.getTime()+10000*1000+10);
    //    msg.put("logTimeFormat", DateUtil.format(new Date(msg.getLong("logTime"))));
    //    msg.put("OutFlow",1000);
    //    msg.put("InFlow",1000);
    //    memorySource.getMemoryCache().addMessage(msg);
    //
    //    Thread.sleep(1000000l);
    //}

}
