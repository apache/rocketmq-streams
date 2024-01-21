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
package org.apache.rocketmq.streams.storage;

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.streams.common.datatype.StringDataType;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.metadata.MetaDataField;
import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.apache.rocketmq.streams.storage.orc.ORCFile;
import org.apache.rocketmq.streams.storage.orc.RowIterator;
import org.junit.Test;

public class ORCFileTest {
    protected static String fieldNames =
        "aliUid\n" +
            "buyAegis\n" +
            "buySas\n" +
            "client_mode\n" +
            "cmd_chain\n" +
            "cmd_chain_index\n" +
            "cmd_index\n" +
            "cmdline\n" +
            "comm\n" +
            "containerhostname\n" +
            "containermip\n" +
            "containername\n" +
            "cwd\n" +
            "data_complete\n" +
            "delta_t1\n" +
            "delta_t2\n" +
            "docker_file_path\n" +
            "dockercontainerid\n" +
            "dockerimageid\n" +
            "dockerimagename\n" +
            "egroup_id\n" +
            "egroup_name\n" +
            "euid\n" +
            "euid_name\n" +
            "file_gid\n" +
            "file_gid_name\n" +
            "file_name\n" +
            "file_path\n" +
            "file_uid\n" +
            "file_uid_name\n" +
            "gcLevel\n" +
            "gid\n" +
            "gid_name\n" +
            "host_uuid\n" +
            "index\n" +
            "k8sclusterid\n" +
            "k8snamespace\n" +
            "k8snodeid\n" +
            "k8snodename\n" +
            "k8spodname\n" +
            "logTime\n" +
            "log_match\n" +
            "parent_cmd_line\n" +
            "parent_file_name\n" +
            "parent_file_path\n" +
            "pcomm\n" +
            "perm\n" +
            "pid\n" +
            "pid_start_time\n" +
            "ppid\n" +
            "scan_time\n" +
            "sid\n" +
            "srv_cmd\n" +
            "stime\n" +
            "tty\n" +
            "uid\n" +
            "uid_name\n";
    protected MetaData metaData;

    public ORCFileTest() {
        String[] columns = fieldNames.split("\n");
        this.metaData = new MetaData();
        for (String column : columns) {
            MetaDataField metaDataField = new MetaDataField();
            metaDataField.setFieldName(column);
            metaDataField.setDataType(new StringDataType());
            metaData.getMetaDataFields().add(metaDataField);
        }
        metaData.setIndexFieldNamesList(new ArrayList<>());
        metaData.getIndexFieldNamesList().add("aliUid");
        metaData.getIndexFieldNamesList().add("cmdline");
        metaData.getIndexFieldNamesList().add("file_name");
        metaData.getIndexFieldNamesList().add("file_path");
        metaData.getIndexFieldNamesList().add("host_uuid");
        metaData.getIndexFieldNamesList().add("parent_cmd_line");
        metaData.getIndexFieldNamesList().add("parent_file_name");
        metaData.getIndexFieldNamesList().add("parent_file_path");

    }

    @Test
    public void testWrite() {
        long start = System.currentTimeMillis();
        List<String> rows = FileUtil.loadFileLine("/Users/yuanxiaodong/aegis_proc_public_1G.txt");
        System.out.println("start writing");
        for (Object object : metaData.getMetaDataFields()) {
            MetaDataField metaDataField = (MetaDataField) object;
            testColumnWrite(metaDataField.getFieldName(), metaDataField.getDataType().getDataTypeName(), rows);
            System.out.println("finish " + metaDataField.getFieldName());
        }

        System.out.println("insert rocksdb cost " + (System.currentTimeMillis() - start));

    }

    public void testColumnWrite(String columnName, String typeName, List<String> rows) {
        MetaData metaData = new MetaData();
        metaData.addMetaDataField("rowId", "long", true);
        metaData.addMetaDataField(columnName, typeName, false);

        ORCFile orcFile = new ORCFile("/Users/yuanxiaodong/orc_1/orc_" + columnName + ".orc", metaData);

        List<JSONObject> msgs = new ArrayList<>();
        long start = System.currentTimeMillis();
        long rowId = 0;
        for (int i = 0; i < 10; i++) {
            for (String rowStr : rows) {
                JSONObject row = JSONObject.parseObject(rowStr);
                JSONObject columnRow = new JSONObject();
                columnRow.put(columnName, row.get(columnName));
                columnRow.put("rowId", rowId++);
                msgs.add(columnRow);
                if (msgs.size() % 1000 == 0) {
                    orcFile.insertData(msgs);
                    msgs = new ArrayList<>();
                }
            }
            if (msgs.size() > 0) {
                orcFile.insertData(msgs);
                msgs = new ArrayList<>();
            }
        }
        orcFile.flush();
    }

    @Test
    public void testRead() {
        MetaData metaData = new MetaData();
        metaData.addMetaDataField("rowId", "long", true);
        metaData.addMetaDataField("buyAegis", "string", false);
        ORCFile orcFile = new ORCFile("/Users/yuanxiaodong/orc/orc_buyAegis.orc", metaData);
        long start = System.currentTimeMillis();
        RowIterator rowIterator = orcFile.queryRowsByTerms("rowId", "buyAegis", false, "1000");
        int count = 0;
        if (rowIterator != null) {
            while (rowIterator.hasNext()) {
                List<JSONObject> jsonObjects = rowIterator.next();
                for (JSONObject row : jsonObjects) {
                    count++;
                }
            }
        }

        System.out.println("query count is " + count + ", cost is " + (System.currentTimeMillis() - start));
    }
}
