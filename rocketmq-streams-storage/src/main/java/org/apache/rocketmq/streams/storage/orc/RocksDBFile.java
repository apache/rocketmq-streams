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
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.metadata.MetaDataField;
import org.apache.rocketmq.streams.state.kv.rocksdb.RocksDBOperator;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

public class RocksDBFile {
    protected static String ROW_PREFIX = "_row";
    protected static String COLUMN_PREFIX = "_columm";

    protected String rocksdbFilePath;
    protected MetaData metaData;
    protected transient RocksDB rocksDB;
    protected Map<String, Integer> columnIndexs = new HashMap<>();

    public RocksDBFile(String rocksdbFilePath, MetaData metaData) {
        this.rocksdbFilePath = rocksdbFilePath;
        this.metaData = metaData;
        List<MetaDataField> metaDataFields = metaData.getMetaDataFields();
        int columnIndex = 0;
        for (MetaDataField metaDataField : metaDataFields) {
            columnIndexs.put(metaDataField.getFieldName(), columnIndex++);

        }
        rocksDB = new RocksDBOperator(rocksdbFilePath).getInstance();
    }

    public void write2RocksDB(List<String> rows) {
        try {
            WriteBatch writeBatch = new WriteBatch();
            List<String> columnIndexList = metaData.getIndexFieldNamesList();
            long rowId = 0;
            for (String row : rows) {
                JSONObject rowJson = JSONObject.parseObject(row);
                if (columnIndexList != null) {

                    for (String columnName : columnIndexList) {
                        Object colummValue = rowJson.get(columnName);
                        MetaDataField metaDataField = metaData.getMetaDataField(columnName);

                        if (colummValue != null) {
                            String value = metaDataField.getDataType().toDataJson(colummValue);
                            String key = columnIndexs.get(columnName) + "," + value + "," + rowId;
                            writeBatch.put(key.getBytes("UTF-8"), new byte[1]);
                        }
                        rowJson.remove(columnName);
                    }
                }

                writeBatch.put(((rowId++) + "").getBytes("UTF-8"), rowJson.toJSONString().getBytes("UTF-8"));

            }
            WriteOptions writeOptions = new WriteOptions();
            writeOptions.setSync(false);
            writeOptions.setDisableWAL(true);
            rocksDB.write(writeOptions, writeBatch);
            writeBatch.close();
            writeOptions.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void write2RocksDB(long rowId, JSONObject row) {
        try {

            WriteBatch writeBatch = new WriteBatch();
            List<String> columnIndexList = metaData.getIndexFieldNamesList();
            if (columnIndexList != null) {
                for (String columnName : columnIndexList) {
                    Object colummValue = row.get(columnName);
                    MetaDataField metaDataField = metaData.getMetaDataField(columnName);

                    if (colummValue != null) {
                        String value = metaDataField.getDataType().toDataJson(colummValue);
                        String key = columnIndexs.get(columnName) + "," + value + "," + rowId;
                        writeBatch.put(key.getBytes("UTF-8"), new byte[1]);
                        if (writeBatch.getDataSize() > 1000) {

                        }
                    }
                    row.remove(columnName);
                }
            }

            writeBatch.put((rowId + "").getBytes("UTF-8"), row.toJSONString().getBytes("UTF-8"));
            WriteOptions writeOptions = new WriteOptions();
            writeOptions.setSync(false);
            writeOptions.setDisableWAL(true);
            rocksDB.write(writeOptions, writeBatch);
            writeBatch.close();
            writeOptions.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("write 2 rocksdb error. rowId=" + rowId);
        }

    }

    public RowIterator queryRowsFromRocksDB(String[] columnNames) {
        throw new RuntimeException("can not support this method");
    }

    public Iterator<List<JSONObject>> queryRowsFromRocksDB(String columnName, DataType dataType, boolean supportPrefix, Object[] terms) {
        return new RocksDBIteratorByTerms(columnName, dataType, terms);
    }

    public Iterator<List<JSONObject>> queryRowsFromRocksDB(String columnName, DataType dataType, Object min, Object max) {
        return new RocksDBIterator(columnName, min, max, dataType);
    }

    protected JSONObject createRowByKey(String columnName, String key) {
        String[] columns = key.split(",");
        String columnIndex = columns[0];
        String rowIndex = columns[columns.length - 1];
        JSONObject row = new JSONObject();
        row.put(columnName, key.substring(columnIndex.length(), key.length() - rowIndex.length()));
        row.put("rowId", rowIndex);
        return row;
    }

    public class RocksDBIteratorByTerms implements Iterator<List<JSONObject>> {
        protected int currentIndex;
        List<RocksDBIterator> rocksDBIterators = new ArrayList<>();

        public RocksDBIteratorByTerms(String columnName, DataType dataType, Object... terms) {
            if (terms != null) {
                for (Object term : terms) {
                    rocksDBIterators.add(new RocksDBIterator(columnName, term, term, dataType));
                }
            }
        }

        @Override public boolean hasNext() {
            if (currentIndex >= rocksDBIterators.size()) {
                return false;
            }
            if (currentIndex < rocksDBIterators.size() - 1) {
                return true;
            }
            if (currentIndex == rocksDBIterators.size() - 1) {
                RocksDBIterator rocksDBIterator = rocksDBIterators.get(currentIndex);
                return rocksDBIterator.hasNext;
            }
            return true;
        }

        @Override public List<JSONObject> next() {
            RocksDBIterator rocksDBIterator = rocksDBIterators.get(currentIndex);
            if (rocksDBIterator.hasNext) {
                rocksDBIterator.next();
            } else {
                currentIndex++;
            }
            if (hasNext()) {
                return next();
            }
            return null;
        }
    }

    public class RocksDBIterator implements Iterator<List<JSONObject>> {
        protected volatile boolean hasNext = true;
        protected AtomicBoolean hasInit = new AtomicBoolean(false);
        protected String min;
        protected String max;
        protected DataType dataType;
        protected String columnName;
        ReadOptions readOptions = new ReadOptions();
        private RocksIterator iter;

        public RocksDBIterator(String columnName, Object minObject, Object maxObject, DataType dataType) {
            readOptions.setPrefixSameAsStart(true).setTotalOrderSeek(true);
            iter = rocksDB.newIterator(readOptions);
            this.columnName = columnName;
            this.min = dataType.toDataJson(minObject);
            if (maxObject == null) {
                this.max = min;
            } else {
                this.max = dataType.toDataJson(maxObject);
                ;
            }
            this.dataType = dataType;
        }

        @Override public boolean hasNext() {
            if (hasInit.compareAndSet(false, true)) {
                iter.seek(min.getBytes());
            }
            return iter.isValid() && hasNext;
        }

        @Override public List<JSONObject> next() {
            String key = new String(iter.key());
            if (!key.startsWith(max)) {
                hasNext = false;
                return null;
            }
            int batchSize = 1000;
            int currentSize = 0;
            List<JSONObject> rows = new ArrayList<>();
            while (currentSize < batchSize && key.startsWith(max)) {
                JSONObject row = createRowByKey(columnName, key);
                iter.next();
                currentSize++;
                rows.add(row);
                key = new String(iter.key());
            }
            if (!key.startsWith(max)) {
                hasNext = false;

            }
            return rows;
        }

    }

}

