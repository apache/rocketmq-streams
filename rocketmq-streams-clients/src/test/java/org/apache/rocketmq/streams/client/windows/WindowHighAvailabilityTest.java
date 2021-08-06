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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.streams.client.StreamBuilder;
import org.apache.rocketmq.streams.client.transform.DataStream;
import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.db.driver.batchloader.BatchRowLoader;
import org.apache.rocketmq.streams.db.driver.batchloader.IRowOperator;
import org.apache.rocketmq.streams.db.driver.orm.ORMUtil;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.operator.impl.WindowOperator;
import org.apache.rocketmq.streams.window.state.impl.WindowValue;
import org.apache.rocketmq.streams.window.storage.WindowStorage;
import org.junit.Test;

public class WindowHighAvailabilityTest extends AbstractWindowFireModeTest {
    protected DataStream createSourceDataStream(){
        return StreamBuilder.dataStream("namespace", "name1")
            .fromFile("/Users/yuanxiaodong/chris/sls_100000.txt",false);
    }

    /**
     *
     * @throws InterruptedException
     */
    @Test
    public void testWindowFireMode0AndNoTimeField() throws InterruptedException {
        super.testWindowFireMode0(false,60*60);
    }



    @Test
    public void testWindowFireMode1() throws InterruptedException {
        super.testWindowFireMode1(false,60*5);
    }



    @Test
    public void testWindowFireMode2() {
        super.testWindowFireMode2(false);
    }

    @Test
    public void testLoadData() throws InterruptedException {
        Map<String,Integer> map=create();
        WindowOperator windowOperator=new WindowOperator();
        windowOperator.setNameSpace("namespace");
        windowOperator.setConfigureName("name1_window_10001");
        WindowInstance windowInstance=windowOperator.createWindowInstance("2021-07-14 02:00:00","2021-07-14 03:00:00","2021-07-14 03:00:00","1");

        WindowStorage storage=  new WindowStorage();
        System.out.println("1;namespace;name1_window_10001;name1_window_10001;2021-07-14 02:00:00;2021-07-14 03:00:00");
        System.out.println(windowInstance.createWindowInstanceId());
        System.out.println(StringUtil.createMD5Str(windowInstance.createWindowInstanceId()));
        Map<String, WindowValue> windowValueMap=new HashMap<>();
        AtomicInteger sum=new AtomicInteger(0);
        BatchRowLoader batchRowLoader = new BatchRowLoader("partition_num",
            "select * from " + ORMUtil.getTableName(WindowValue.class) + "  where window_instance_partition_id ='"
                + StringUtil.createMD5Str(windowInstance.createWindowInstanceId() )+ "'", new IRowOperator(){

                @Override
                public void doProcess(Map<String, Object> row) {
                    WindowValue windowValue = ORMUtil.convert(row, WindowValue.class);

                    String groupBy=windowValue.getGroupBy();
                    windowValueMap.put(groupBy,windowValue);
                    if(!map.containsKey(groupBy)){

                        System.out.println(groupBy+" ======");
                    }else {
                        Integer count=(Integer)windowValue.getComputedColumnResultByKey("total");
                        int mapCount=map.get(groupBy);
                        if(count!=mapCount){
                            System.out.println(groupBy+" ======");
                        }
                        //System.out.println(groupBy+" "+count);
                        sum.addAndGet(count);

                    }
                }
            });
        batchRowLoader.startLoadData();

        System.out.println("sum "+sum.get());
        for(String key:map.keySet()){
            if(windowValueMap.containsKey(key)==false){
                System.out.println(key+" "+map.get(key));
            }
        }

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



}
