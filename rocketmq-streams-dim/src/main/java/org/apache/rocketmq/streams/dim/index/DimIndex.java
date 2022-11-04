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
package org.apache.rocketmq.streams.dim.index;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.cache.compress.AbstractMemoryTable;
import org.apache.rocketmq.streams.common.cache.compress.KVAddress;
import org.apache.rocketmq.streams.common.cache.compress.impl.MapAddressListKV;
import org.apache.rocketmq.streams.common.datatype.IntDataType;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DimIndex {

    private static final Logger LOGGER = LoggerFactory.getLogger(DimIndex.class);

    /**
     * 索引字段名，支持多个索引，每个索引一行，支持组合索引，多个字段用；拼接 name 单索引 name;age 组合索引
     */
    protected List<String> indexs = new ArrayList<>();

    /**
     * 如果是非唯一索引，用这个结构存储 每个索引一行，后面的map：key：索引值；value：row id 列表，rowid用字节表示
     */
    protected Map<String, MapAddressListKV> mutilIndex = new HashMap<>();

    public DimIndex(List<String> indexs) {
        this.indexs = formatIndexs(indexs);
    }

    public DimIndex(String index, String... indexs) {
        if (indexs == null) {
            return;
        }
        List<String> indexList = new ArrayList<>();
        for (String idx : indexs) {
            indexList.add(idx);
        }
        this.indexs = formatIndexs(indexList);
    }

    /**
     * 组合索引，多个字段要以名称顺序排列，完成索引名称的标注化处理
     *
     * @param indexs
     * @return
     */
    protected List<String> formatIndexs(List<String> indexs) {
        List<String> allIndex = new ArrayList<>();
        for (String indexName : indexs) {
            String[] values = indexName.split(";");
            List<String> indexList = new ArrayList<>();
            for (String value : values) {
                indexList.add(value);
            }
            Collections.sort(indexList);
            String indexKey = MapKeyUtil.createKey(indexList);
            allIndex.add(indexKey);
        }
        return allIndex;
    }

    /**
     * 加载一行数据，如果是唯一索引，在uniqueIndex中查找，否则在mutilIndex查找
     *
     * @param indexName  索引名，如name
     * @param indexValue 索引值，如chris
     * @return
     */
    public List<Long> getRowIds(String indexName, String indexValue) {
        MapAddressListKV indexs = this.mutilIndex.get(indexName);
        if (indexs == null) {
            return null;
        }
        return indexs.getLongValue(indexValue);
    }

    /**
     * 构建索引，如果是唯一索引，构建在uniqueIndex数据结构中，否则构建在mutilIndex这个数据结构中
     *
     * @param tableCompress 表数据
     */
    public void buildIndex(AbstractMemoryTable tableCompress) {

        Iterator<AbstractMemoryTable.RowElement> it = tableCompress.newIterator();
        int i = 0;
        while (it.hasNext()) {
            AbstractMemoryTable.RowElement row = it.next();
            long rowIndex = row.getRowIndex();
//            KVAddress mapAddress = new KVAddress(new ByteArray(NumberUtils.toByte(rowIndex)));
            KVAddress mapAddress = KVAddress.createMapAddressFromLongValue(rowIndex);
            addRowIndex(row.getRow(), mapAddress, tableCompress.getRowCount());
//            addRowIndex(row.getRow(),row.getRowIndex(),tableCompress.getRowCount());
            if ((i % 100000) == 0) {
                LOGGER.debug("dim build continue...." + i);
            }
            i++;
        }
        LOGGER.debug(" finish poll data , the row count  is " + i + ". byte is " + tableCompress.getByteCount());
    }

    /**
     * 如果想直接增加索引，可以用这个方法
     *
     * @param row
     * @param rowIndex
     * @param rowSize
     */
    public void addRowIndex(Map<String, Object> row, KVAddress rowIndex, int rowSize) {
        Map<String, String> cacheValues = createRow(row);
        if (indexs == null || indexs.size() == 0) {
            return;
        }
        for (String indexName : indexs) {
            MapAddressListKV name2RowIndexs = this.mutilIndex.get(indexName);
            if (name2RowIndexs == null) {
                synchronized (this) {
                    name2RowIndexs = this.mutilIndex.get(indexName);
                    if (name2RowIndexs == null) {
                        name2RowIndexs = new MapAddressListKV(rowSize);
                        this.mutilIndex.put(indexName, name2RowIndexs);
                    }
                }

            }
            String[] nameIndexs = indexName.split(";");
            Arrays.sort(nameIndexs);
            String indexValue = createIndexValue(cacheValues, nameIndexs);
            name2RowIndexs.add(indexValue, rowIndex);
        }
    }

    /**
     * 对于组合索引，把各个字段的值取出来
     *
     * @param row
     * @param nameIndexs
     * @return
     */
    protected String createIndexValue(Map<String, String> row, String[] nameIndexs) {
        String[] indexValues = new String[nameIndexs.length];
        for (int i = 0; i < nameIndexs.length; i++) {
            indexValues[i] = row.get(nameIndexs[i]);
        }
        if (indexValues.length > 0) {
            return MapKeyUtil.createKey(indexValues);
        }
        return null;
    }

    /**
     * 把row 中非string的值转化成string
     *
     * @param row
     * @return
     */
    protected Map<String, String> createRow(Map<String, Object> row) {
        Map<String, String> cacheValues = new HashMap<String, String>();//一行数据
        //把数据value从object转化成string
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            if (entry != null && entry.getValue() != null && entry.getKey() != null) {
                cacheValues.put(entry.getKey(), entry.getValue().toString());
            }
        }
        return cacheValues;
    }

    public static IntDataType INTDATATYPE = new IntDataType();

    public List<String> getIndexs() {
        return indexs;
    }

    public void setIndexs(List<String> indexs) {
        this.indexs = indexs;
    }

}
