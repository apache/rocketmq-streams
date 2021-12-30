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
package org.apache.rocketmq.streams.common.cache.compress;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.cache.MappedByteBufferTable;
import org.apache.rocketmq.streams.common.cache.TableSchema;
import org.junit.Test;

/**
 * @description
 */
public class MappedByteBufferTest extends BaseTest {

    TableSchema schema = new TableSchema(
        new String[] {"key", "value"},
        new String[] {"string", "string"});

    @Test
    public void testReadAndWrite1() {
        MappedByteBufferTable compressTable = new MappedByteBufferTable("test_job", 5, null);
        compressTable.setFileSize(1024);
        Object[] data = new Object[5];
        data[0] = Integer.parseInt("1");
        data[1] = Integer.parseInt("2");
        data[2] = Integer.parseInt("3");
        data[3] = Integer.parseInt("4");
        data[4] = Integer.parseInt("5");

        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < data.length; i++) {
            map.put(String.valueOf(i), data[i]);
        }
        long index = compressTable.addRow(map);
        Map<String, Object> ret = compressTable.getRow(index);
        System.out.println(ret);

    }

    @Test
    public void testReadAndWrite2() {
        String key = "11111";
        String value = "22222";
        System.out.println(Arrays.toString(key.getBytes()));
        System.out.println(Arrays.toString(value.getBytes()));
//        String value = "304558289311139925134857270963359080830254370985486304849340901936353706956569165408355875140966952470058707558697517774382494553413390684141445702257728316363854847781419149813013938273636285539656183542745414091365062328829045076923172722374029086334929295737938700980256663413748237741796610633914154757445964208314294707388119063701139517067102493823976557337401174815093462386993117219332665638898729365577029560017086535299871527517859696733162705495635271148423670438930417098998256995348436140791977116018271098302448801356097432958628690659302200811638093895815411465603361765488595638101871488753297617915668886212125282912584711755029219186064716733869803328404962802335348998650413974144680946235021111093396192897401922277460389457804508526347474700989152756852399530669894651283468738872141302345384976551826364";
        Map<String, Object> map = new HashMap<>();
        map.put("key", key);
        map.put("value", value);
        MappedByteBufferTable table = new MappedByteBufferTable("test_job", 2, schema);
//        ByteArrayMemoryTable table = new ByteArrayMemoryTable();
        table.setFileSize(1024);
        long index = table.addRow(map);
        Map<String, Object> ret = table.getRow(index);
        System.out.println(ret);
    }

    @Test
    public void testWrite1() {
        MappedByteBufferTable compressTable = new MappedByteBufferTable("test_read_write", 2, null);
        compressTable.setFileSize(1024 * 1024); // 32M
        List<Map<String, Object>> datas = new ArrayList<>();
        writeTwoColumnsData(10000, compressTable, datas, "test_read_write");
    }

    @Test
    public void testReadAndWrite3() {
        String fileName = "test_read_write1";
        MappedByteBufferTable compressTable = new MappedByteBufferTable(fileName, 2, new TableSchema(new String[] {
            "key", "value"
        }, new String[] {"string", "string"}));
        compressTable.setFileSize(1024 * 1024); // 1M
        List<Map<String, Object>> datas = new ArrayList<>();
        List<Long> offsets = writeTwoColumnsData(100000, compressTable, datas, fileName);
        String key = "value";
        for (int i = 0; i < offsets.size(); i++) {
            long offset = offsets.get(i);
            System.out.println(offset);
            Map<String, Object> fileData = compressTable.getRow(offset);
            Map<String, Object> rawData = datas.get(i);
            Object value1 = rawData.get(key);
            Object value2 = fileData.get(key);
            if (!value1.equals(value2)) {
                System.out.println("error : " + value1 + ", " + value2);
            }
        }
    }

    @Test
    public void testRead01() {
        String fileName = "test_read_write";
        List<Long> offsets = readOffsetFromFile(fileName);
        MappedByteBufferTable table = new MappedByteBufferTable(fileName, 2, new TableSchema(new String[] {
            "key", "value"
        }, new String[] {"string", "string"}));
        int i = 0;
        for (Long offset : offsets) {
            String keyData = "key" + i;
            System.out.println(keyData);
            Map<String, Object> row = table.getRow(offset);
            Object key = row.get("key");
            Object value = row.get("value");

            if (!keyData.equals(value.toString())) {
                System.out.println("error : " + keyData + "！= " + key);
            }
            i++;
        }
    }

    @Test
    public void test() {
        File file = new File("/tmp");
        List<String> pos = new ArrayList<>();
        for (File f : file.listFiles()) {
            String fileName = f.getName();
            if (fileName.startsWith("dipper_test_read_write")) {
//                String posStr = fileName.replace("dipper_test_read_write_", "");
                pos.add(fileName);
            }
        }
        String[] array = pos.toArray(new String[0]);
        Arrays.sort(array);
        System.out.println(Arrays.toString(array));
    }

    @Test
    public void testReadAndWrite4() {
        String fileName = "test_job_12";
        MappedByteBufferTable compressTable = new MappedByteBufferTable(fileName, 2, new TableSchema(new String[] {"key", "value"}, new String[] {"key1", "value1"}));
        compressTable.setFileSize(1024);
        String longValue1 = "9199364479266898464382449947021185370172829533611814713118796945634954454921233312918031220970453551639792435744579228762581283198901120154657247953317654447541807125341154375042617696478716344543928058700968245533623906734561782581150301671937670420397596806498174147523844963059811348415420655882247933378431525839775535813907552333508101880513011341459762043333191799673086561261255086483592865635258000181548127100169607504868954628618652269557975370315360949508258434851389666157592925317979575325";
        String longValue2 = "670283667448617954161320820799014526853295720707903080629740349433467466789098258821136791770902568610463861271221422564135408096812260229286958030534305580624539143002995297762911458129458218781073258190598001797846489533606719109519460033134462719004050446175840830509846081966801309308241533044191926716737370569878386972004145446804035462495340644127715464669623302521392606434023597719456315583190317050452097694102009438633047871680198380774537294001577942520785463554152194420722464084839648780381381472156625043405225199747254374479187031027647516746658705719734309876";
        Map<String, Object> map1 = new HashMap<>();
        map1.put("key", "key1");
        map1.put("value", "value1");

        Map<String, Object> map2 = new HashMap<>();
        map2.put("key", "key2");
        map2.put("value", "");

        Map<String, Object> map3 = new HashMap<>();
        map3.put("key", "key3");
        map3.put("value", longValue1);

        Map<String, Object> map4 = new HashMap<>();
        map4.put("key", "key4");
        map4.put("value", "value4");

        Map<String, Object> map5 = new HashMap<>();
        map5.put("key", "key5");
        map5.put("value", longValue2);

        Map<String, Object> map6 = new HashMap<>();
        map6.put("key", "key6");
        map6.put("value", "value6");

        long index = compressTable.addRow(map1);
        System.out.println(compressTable.getCaches().get(0).position());
        long index1 = compressTable.addRow(map2);
        System.out.println(compressTable.getCaches().get(0).position());
        long index2 = compressTable.addRow(map3);
        System.out.println(compressTable.getCaches().get(0).position());
        long index3 = compressTable.addRow(map4);
        System.out.println(compressTable.getCaches().get(0).position());
        long index4 = compressTable.addRow(map5);
        System.out.println(compressTable.getCaches().get(0).position());
        long index5 = compressTable.addRow(map6);
        System.out.println(compressTable.getCaches().get(0).position());

        System.out.println("total size is " + compressTable.getByteTotalSize());

//        MappedByteBuffer buffer = compressTable.getCaches().get(0);
//        int i = 0;
//        buffer.position(0);
//        while(buffer.hasRemaining()){
//            System.out.println(buffer.get() + "---------" + (i++));
//        }

        Map<String, Object> ret = compressTable.getRow(index);
        System.out.println(ret);

        Map<String, Object> ret1 = compressTable.getRow(index1);
        System.out.println(ret1);

        Map<String, Object> ret2 = compressTable.getRow(index2);
        System.out.println(ret2);

        Map<String, Object> ret3 = compressTable.getRow(index3);
        System.out.println(ret3);

        Map<String, Object> ret4 = compressTable.getRow(index4);
        System.out.println(ret4);

        Map<String, Object> ret5 = compressTable.getRow(index5);
        System.out.println(ret5);

//cmdline=/sbin/udevd  -d, filepath=/usr/sbin/udevd , pid=10551, host_uuid=d23ccffb-615c-42e6-9920-e37b11760444, sid=10548

    }
//
//    @Testq
//    public void testReadAndWriteLoop(){
//        MappedByteBufferTable compressTable = new MappedByteBufferTable();
//        compressTable.setJobName("test_job_loop");
//        compressTable.setFileSize(18);
//
//        List<Integer> indexs = new ArrayList<>();
//
//        for(int i = 0; i < 10; i++){
//            Object[] data = new Object[5];
//            data[0] = 1;
//            data[1] = 2;
//            data[2] = 3;
//            data[3] = 4;
//            data[4] = 5;
//            Map<String, Object> map = new HashMap<>();
//            for(int j = 0; j < data.length; j++){
//                map.put(String.valueOf(j), data[j]);
//            }
//            int index = compressTable.addRow(map);
//            indexs.add(index);
//            System.out.println("--------------" + i + "-----------------");
//            System.out.println("return index is " + index);
//            System.out.println("cursor is " + compressTable.getCursor());
//            System.out.println("totol size is " + compressTable.getByteTotalSize());
//
//        }
//
//        for(int i = 0; i < indexs.size(); i++){
//            System.out.println("-------------- read " + i + "-----------------");
//            int index = indexs.get(i);
//            Map<String, Object> row = compressTable.getRow(index);
//            System.out.println(row);
//        }
//
//    }

}
