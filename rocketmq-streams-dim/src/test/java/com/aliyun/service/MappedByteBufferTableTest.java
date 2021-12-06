package com.aliyun.service;

import org.apache.rocketmq.streams.common.cache.MappedByteBufferTable;
import org.apache.rocketmq.streams.common.cache.compress.AbstractMemoryTable;
import org.apache.rocketmq.streams.dim.index.DimIndex;
import org.junit.Test;

import java.util.*;

/**
 * @author zengyu.cw
 * @program rocketmq-streams-apache
 * @create 2021-11-30 10:57:05
 * @description
 */
public class MappedByteBufferTableTest {

    static {
        System.setProperty("log4j.home", System.getProperty("user.home") + "/logs");
        System.setProperty("log4j.level", "INFO");

    }

    @Test
    public void testReadAndWriteWithDim(){
        MappedByteBufferTable compressTable = new MappedByteBufferTable();
        compressTable.setJobName("test_job_dim");
        compressTable.setFileSize(1024 * 1024 * 256);

        List<Integer> indexs = new ArrayList<>();

        long time1 = System.currentTimeMillis();
        for(int i = 0; i < 30000000; i++){
            Object[] data = new Object[5];
            data[0] = String.valueOf(1 + i);
            data[1] = "2";
            data[2] = "3";
            data[3] = "4";
            data[4] = "5";
            Map<String, Object> map = new HashMap<>();
            for(int j = 0; j < data.length; j++){
                map.put(String.valueOf("field" + j), data[j]);
            }
            int index = compressTable.addRow(map);
            indexs.add(index);
//            System.out.println("--------------" + i + "-----------------");
//            System.out.println("return index is " + index);
//            System.out.println("cursor is " + compressTable.getCursor());
//            System.out.println("totol size is " + compressTable.getByteTotalSize());

        }
        long time2 = System.currentTimeMillis();
        System.out.println("write " + (time2 - time1)/(1000 * 60.0f) + ", size = " + compressTable.getByteTotalSize());
//
        List<String> indexName = new ArrayList<>();
        indexName.add("field0");
        DimIndex dimIndex = new DimIndex(indexName);
        dimIndex.buildIndex(compressTable);

        int total = 0;
//        for(int i = 0; i < indexs.size(); i++){
////            System.out.println("-------------- read " + i + "-----------------");
//            int index = indexs.get(i);
//            Map<String, Object> row = compressTable.getRow(index);
//            if(row != null){
//                total++;
//            }
////            System.out.println(row);
//        }
//        Iterator<AbstractMemoryTable.RowElement> it = compressTable.newIterator();
//        while(it.hasNext()){
//            it.next();
//            total++;
//        }
        long time3 = System.currentTimeMillis();
        System.out.println("read " + (time3 - time2)/(1000 * 60.0) + ", total " + total);

    }
}
