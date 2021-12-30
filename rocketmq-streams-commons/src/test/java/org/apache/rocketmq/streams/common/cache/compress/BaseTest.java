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
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.rocketmq.streams.common.cache.FileBasedTable;

public class BaseTest {

    public List<Long> writeTwoColumnsData(int rowSize, FileBasedTable table, List<Map<String, Object>> rows,
        String filePath) {

        long start = System.currentTimeMillis();
        long total1 = 0;
        long total2 = 0;
        long total3 = 0;
        List<Long> offsets = new ArrayList<>();
        for (int i = 0; i < rowSize; i++) {
            long start1 = System.currentTimeMillis();
            Map<String, Object> row = new HashMap<String, Object>();
            long end1 = System.currentTimeMillis();
            String key = "key" + i;
            String value = getStringValue();
            long end2 = System.currentTimeMillis();
            row.put("key", key);
            row.put("value", value);
            long end3 = System.currentTimeMillis();
            total1 = total1 + (end1 - start1);
            total2 = total2 + (end2 - end1);
            total3 = total3 + (end3 - end2);
            long offset = table.addRow(row);
            if (rows != null) {
                rows.add(row);
                offsets.add(offset);
            }
        }
        long end = System.currentTimeMillis();
        long time = (end - start);
        System.out.println("total row count is " + rowSize);
        System.out.println("total byte size is " + table.getFileOffset());
        System.out.println("total time is " + time);
        System.out.println("new map time is " + total1);
        System.out.println("gen data time is  " + total2);
        System.out.println("put map time is " + total3);
        if (rows != null) {
            try {
                writeMap2File(rows, filePath);
                writeOffset2File(offsets, filePath);
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        return offsets;
    }

    public void writeMap2File(List<Map<String, Object>> rows, String path) throws IOException {
        List<String> lines = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            lines.add(row.get("key") + "," + row.get("value"));
        }
        path = "/tmp/" + path + "_data";
        File file = getFile(path);
        FileUtils.writeLines(file, lines);
    }

    public void writeOffset2File(List<Long> offsets, String path) throws IOException {
        path = "/tmp/" + path + "_offset";
        File file = getFile(path);
        FileUtils.writeLines(file, offsets);
    }

    public List<Map<String, Object>> readMapFromFile(String path) {
        path = "/tmp/" + path + "_data";
        List<String> lines = null;
        try {
            File file = getFile(path);
            lines = FileUtils.readLines(file);
        } catch (IOException e) {
            e.printStackTrace();
        }
        List<Map<String, Object>> rows = new ArrayList<>();
        for (String line : lines) {
            String[] rowLine = line.split(",");
            Map<String, Object> row = new HashMap<>();
            row.put("key", rowLine[0]);
            row.put("value", rowLine[1]);
            rows.add(row);
        }
        return rows;
    }

    public List<Long> readOffsetFromFile(String path) {
        path = "/tmp/" + path + "_offset";
        List<String> lines = null;
        try {
            File file = getFile(path);
            lines = FileUtils.readLines(file);
        } catch (IOException e) {
            e.printStackTrace();
        }
        List<Long> offsets = new ArrayList<>();
        for (String line : lines) {
            offsets.add(Long.valueOf(line));
        }
        return offsets;
    }

    private String getStringValue() {
        Random random = new Random();
        int length = random.nextInt(1024);
        StringBuilder sb = new StringBuilder("");
        while (length-- > 0) {
            int value = random.nextInt(10);
            sb.append(value);
        }
        return sb.toString();
    }

    private File getFile(String path) throws IOException {
        File file = new File(path);
        if (!file.exists()) {
            file.createNewFile();
        }
        return file;
    }

}
