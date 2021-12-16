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
package com.aliyun.service;

import com.alibaba.fastjson.JSONObject;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.cache.ByteArrayMemoryTable;
import org.apache.rocketmq.streams.common.cache.compress.AbstractMemoryTable;
import org.apache.rocketmq.streams.common.cache.compress.impl.IntListKV;
import org.junit.Test;

import static org.junit.Assert.assertFalse;

public class TableCompressTest {

    @Test
    public void testNameList() throws InterruptedException {
        long startTime=System.currentTimeMillis();
        ByteArrayMemoryTable table=new ByteArrayMemoryTable();
       //加载数据
        for(int i=0;i<20000000;i++){
            JSONObject msg=new JSONObject();
            msg.put("name","chris"+i);
            msg.put("age",i);
            msg.put("address","fddfdsfdsffddsdfsfdsfFDFDFDSDFSFDDFDSFSfddsffdsdsffdsfdfdsfdsfd"+i);
            table.addRow(msg);
        }

        //建立索引和验证索引
       IntListKV index=new IntListKV(table.getRowCount());
        Iterator<AbstractMemoryTable.RowElement> it = table.newIterator();
        while (it.hasNext()){
            AbstractMemoryTable.RowElement rowElement=it.next();
            Map<String,Object> row=rowElement.getRow();
            index.add((String)row.get("name"),rowElement.getRowIndex());
            String key=(String)row.get("name");
            List<Integer>rowIds=index.get(key);
            int rowIndex=rowIds.get(0).intValue();
            Map<String,Object> map=table.getRow(rowIndex);
            if(!map.get("name").equals(row.get("name"))||!map.get("age").equals(row.get("age"))||!map.get("address").equals(row.get("address"))){
                assertFalse("索引可能存在问题，基于索引获取的值和原数据不匹配",true);
            }

        }
        long start=System.currentTimeMillis();
        for(int i=0;i<20000000;i++){
            String name="chris"+i;
            List<Integer> rowIds= index.get(name);
            for(Integer rowId:rowIds){
                table.getRow(rowId.intValue());
            }
        }
        System.out.println("query time cost is "+(System.currentTimeMillis()-start));

        //空间占用
        double indexSize=((double) index.calMemory())/1024;
        System.out.println("原始数据大小(G)："+(((double)table.getByteCount())/1024/1024/1024+indexSize)+"G");
        double size=((double)table.getCache().byteSize())/1024;
        System.out.println("存储空间大小(G)"+(size+indexSize)+"G");
        System.out.println("insert and query cost is "+(System.currentTimeMillis()-startTime));
    }




    @Test
    public void testColumNull() throws InterruptedException {
        ByteArrayMemoryTable table=new ByteArrayMemoryTable();
        JSONObject msg=new JSONObject();
        msg.put("name","chris"+0);
        msg.put("age",0);
        msg.put("address","fddfdsfdsffddsdfsfdsfFDFDFDSDFSFDDFDSFSfddsffdsdsffdsfdfdsfdsfd");
        table.addRow(msg);
        msg.put("address",null);
        int rowIndex=table.addRow(msg);
        Map<String,Object> row=table.getRow(rowIndex);
        System.out.println(row.size());
    }
}
