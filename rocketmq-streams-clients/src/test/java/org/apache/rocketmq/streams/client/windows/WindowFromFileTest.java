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
package org.apache.rocketmq.streams.client.windows;

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.client.StreamBuilder;
import org.apache.rocketmq.streams.client.transform.DataStream;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.PrintUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.db.driver.batchloader.IRowOperator;
import org.apache.rocketmq.streams.db.driver.orm.ORMUtil;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.operator.AbstractWindow;
import org.apache.rocketmq.streams.window.operator.impl.WindowOperator;
import org.apache.rocketmq.streams.window.state.impl.WindowValue;
import org.apache.rocketmq.streams.window.storage.WindowStorage;
import org.junit.Test;

public class WindowFromFileTest extends org.apache.rocketmq.streams.client.windows.AbstractWindowFireModeTest {
    protected DataStream createSourceDataStream(){
        return StreamBuilder.dataStream("namespace", "name1")
            .fromFile("/Users/yuanxiaodong/chris/sls_100000.txt",true);
    }

    /**
     *
     * @throws InterruptedException
     */
    @Test
    public void testWindow() throws InterruptedException {
//        String dir="/tmp/rockstmq-streams-1";
//        FileUtil.deleteFile(dir);
//        ComponentCreator.getProperties().setProperty("window.debug","true");
//        ComponentCreator.getProperties().setProperty("window.debug.dir",dir);
//        ComponentCreator.getProperties().setProperty("window.debug.countFileName","total");
        super.testWindowFireMode0(false);
    }



    @Test
    public void testWindowFireMode1() throws InterruptedException {
        super.testWindowFireMode1(false);
    }



    @Test
    public void testWindowFireMode2() {
        super.testWindowFireMode2(false);
    }



    @Test
    public void testSLS10(){
        Map<String,Integer> map=create();

        System.out.println(map.size());
        PrintUtil.print(map);
    }


    @Test
    public void testLoadData() throws InterruptedException {
        Map<String,Integer> map=create();
        WindowOperator windowOperator=new WindowOperator();
        windowOperator.setNameSpace("namespace");
        windowOperator.setConfigureName("name1");
        WindowInstance windowInstance=windowOperator.createWindowInstance("2021-07-14 02:00:00","2021-07-14 03:00:00","2021-07-14 03:00:00","1");
        WindowStorage storage=  new WindowStorage();
        storage.loadSplitData2Local("1", windowInstance.createWindowInstanceId(), WindowValue.class, new IRowOperator() {
            @Override
            public void doProcess(Map<String, Object> row) {
                WindowValue windowValue = ORMUtil.convert(row, WindowValue.class);
                String groupBy=windowValue.getGroupBy();
                if(!map.containsKey(groupBy)){

                    System.out.println(groupBy+" ======");
                }else {
                    Integer count=(Integer)windowValue.getComputedColumnResultByKey("total");
                    int mapCount=map.get(groupBy);
                    if(count!=mapCount){
                        System.out.println(groupBy+" ======");
                    }
                }
            }
        });


            Thread.sleep(10000000l);
    }


    protected Map<String,Integer> create(){
        List<String> lines= FileUtil.loadFileLine("/Users/yuanxiaodong/chris/sls_10000.txt");
        Map<String,Integer> map=new HashMap<>();
        for(String line:lines){
            JSONObject msg=JSONObject.parseObject(line);
            String projectName=msg.getString("ProjectName");
            String logstore=msg.getString("LogStore");

            String key= MapKeyUtil.createKey(projectName,logstore);

            if (StringUtil.isEmpty(key)) {
                key="<null>";
            }
            Integer count=map.get(key);
            if(count==null){
                map.put(key,1);
            }else {
                count++;
                map.put(key,count);
            }
        }
        return map;
    }


    @Test
    public void testFileResult(){
       createFile(1);
    }


    protected void createFile(int count){

//        date.setYear(2021-1900);
//        date.setMonth(6);
//        date.setDate(27);
//        date.setHours(12);
//        date.setMinutes(1);
//        date.setSeconds(0);
        Long time=null;
        for(int i=0;i<count;i++){
            List<String> lines= FileUtil.loadFileLine("/Users/yuanxiaodong/chris/sls_100000.txt");
            List<String> msgs=new ArrayList<>();
            for(String line:lines){
                JSONObject jsonObject=JSONObject.parseObject(line);
                JSONObject msg=new JSONObject();
                msg.put("ProjectName",jsonObject.getString("ProjectName"));
                msg.put("LogStore",jsonObject.getString("LogStore"));
                msg.put("OutFlow",jsonObject.getString("OutFlow"));
                msg.put("InFlow",jsonObject.getString("InFlow"));
                if(time==null){
                    time=date;
                }else {
                    time=time+1;
                }
                msg.put("logTime",time);
                msg.put("currentTime", DateUtil.format(new Date(time)));

                AbstractWindow window=new WindowOperator();
                window.setSizeInterval(5);
                window.setTimeUnitAdjust(1);
                window.setTimeFieldName("logTime");
                msgs.add(msg.toJSONString());
            }
            FileUtil.write("/Users/yuanxiaodong/chris/sls_88121_"+i+".txt",msgs,false);
        }

    }

}
