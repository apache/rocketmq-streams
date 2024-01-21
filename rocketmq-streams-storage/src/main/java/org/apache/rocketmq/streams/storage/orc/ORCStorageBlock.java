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
package org.apache.rocketmq.streams.storage.orc;

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.storage.IStorageBlock;

public class ORCStorageBlock extends BasedConfigurable implements IStorageBlock {
    protected static String ROW_ID = "_ROW_ID";

    protected MetaData metaData;//字段信息
    protected boolean isWriteFinished = false;//块是否完成写入
    protected AtomicLong size = new AtomicLong(0);//行数
    protected AtomicLong blockByteSize = new AtomicLong(0);//

    /**
     * key:columnName
     */
    protected transient Map<String, ORCFile> orcFileMap = new HashMap<>();
    protected transient RocksDBFile rocksDBFile;

    @Override protected boolean initConfigurable() {
        return super.initConfigurable();
    }

    @Override public MetaData getMetaData() {
        return metaData;
    }

    @Override public long insertData(JSONObject... msgs) {
        if (msgs == null) {
            return blockByteSize.get();
        }
        List<JSONObject> rows = new ArrayList<>();
        for (JSONObject row : msgs) {
            rows.add(row);
        }
        return insertData(rows);
    }

    @Override public long insertData(List<JSONObject> msgs) {
        if (msgs == null) {
            return blockByteSize.get();
        }
        try {
            for (JSONObject msg : msgs) {
                long rowId = size.get();
                blockByteSize.addAndGet(msg.toString().getBytes("UTF-8").length);
                rocksDBFile.write2RocksDB(rowId, msg);
                size.incrementAndGet();
            }
            return blockByteSize.get();
        } catch (Exception e) {
            throw new RuntimeException("insert data 2 rocksdb error ", e);
        }

    }

    @Override public RowIterator queryRows(String... columnNames) {
        if (isWriteFinished) {
            return queryRowsFromSortORC(columnNames);
        } else {
            return rocksDBFile.queryRowsFromRocksDB(columnNames);
        }
    }

    @Override public Iterator<List<JSONObject>> queryRowsByTerms(String columnName, boolean supportPrefix, Object... terms) {
        if (isWriteFinished) {
            return queryRowsFromSortORC(columnName, supportPrefix, terms);
        } else {
            DataType dataType = metaData.getMetaDataField(columnName).getDataType();
            return rocksDBFile.queryRowsFromRocksDB(columnName, dataType, supportPrefix, terms);
        }
    }

    @Override public Iterator<List<JSONObject>> queryRowsByBetween(String columnName, Object queryMin, Object queryMax) {
        if (isWriteFinished) {
            return queryRowsFromSortORC(columnName, queryMin, queryMax);
        } else {
            DataType dataType = metaData.getMetaDataField(columnName).getDataType();
            return rocksDBFile.queryRowsFromRocksDB(columnName, dataType, queryMin, queryMax);
        }
    }

    private RowIterator queryRowsFromSortORC(String[] columnNames) {
        throw new RuntimeException("can not suppoert this method");
    }

    private RowIterator queryRowsFromSortORC(String columnName, boolean supportPrefix, Object[] terms) {
        ORCFile orcFile = orcFileMap.get(columnName);
        return orcFile.queryRowsByTerms(columnName, ROW_ID, supportPrefix, terms);
    }

    private RowIterator queryRowsFromSortORC(String columnName, Object min, Object max) {
        ORCFile orcFile = orcFileMap.get(columnName);
        return orcFile.queryRowsByBetween(columnName, ROW_ID, min, max);
    }

    @Override public long getRowSize() {
        return size.get();
    }

    @Override public void finishWrite() {
        this.isWriteFinished = true;
    }

}
