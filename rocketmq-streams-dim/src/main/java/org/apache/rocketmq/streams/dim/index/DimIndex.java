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

import org.apache.rocketmq.streams.common.datatype.IntDataType;
import org.apache.rocketmq.streams.common.cache.compress.impl.IntValueKV;
import org.apache.rocketmq.streams.common.cache.CompressTable;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DimIndex {

    private static final Log LOG = LogFactory.getLog(DimIndex.class);

    /**
     * 索引字段名，支持多个索引，每个索引一行，支持组合索引，多个字段用；拼接 name 单索引 name;age 组合索引
     */
    protected List<String> indexs = new ArrayList<>();

    /**
     * 如果是唯一索引，用压缩值存储 每个索引一行
     */
    protected Map<String, IntValueKV> uniqueIndex;

    /**
     * 如果是非唯一索引，用这个结构存储 每个索引一行，后面的map：key：索引值；value：row id 列表，rowid用字节表示
     */
    protected Map<String, Map<String, List<Integer>>> mutilIndex = new HashMap<>();

    protected boolean isUnique = false;//如果是唯一索引，值为true

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
    public List<Integer> getRowIds(String indexName, String indexValue) {
        if (isUnique) {
            IntValueKV index = this.uniqueIndex.get(indexName);
            if (index == null) {
                return null;
            }
            Integer rowId = index.get(indexValue);
            if (rowId == null) {
                return null;
            }
            List<Integer> rowIds = new ArrayList<>();
            rowIds.add(rowId);
            return rowIds;
        } else {
            Map<String, List<Integer>> indexs = this.mutilIndex.get(indexName);
            if (indexs == null) {
                return null;
            }
            return indexs.get(indexValue);
        }
    }

    /**
     * 构建索引，如果是唯一索引，构建在uniqueIndex数据结构中，否则构建在mutilIndex这个数据结构中
     *
     * @param tableCompress 表数据
     */
    public void buildIndex(CompressTable tableCompress) {

        if (isUnique) {
            Map<String, IntValueKV> fieldIndex2RowIndex = new HashMap<>();
            buildUniqueIndex(tableCompress, fieldIndex2RowIndex);
            this.uniqueIndex = fieldIndex2RowIndex;
        } else {
            Map<String, Map<String, List<Integer>>> fieldIndex2RowIndex = new HashMap<>();
            buildIndex(tableCompress, fieldIndex2RowIndex);
            this.mutilIndex = fieldIndex2RowIndex;
        }
    }

    /**
     * 构建唯一索引
     *
     * @param tableCompress
     * @param fieldIndex2RowIndex
     */
    protected void buildUniqueIndex(CompressTable tableCompress, Map<String, IntValueKV> fieldIndex2RowIndex) {
        if (indexs == null || indexs.size() == 0) {
            return;
        }

        int i = 0;
        Iterator<Map<String, Object>> it = tableCompress.newIterator();
        while (it.hasNext()) {
            /**
             * 为每个索引做构建
             */
            for (String indexName : indexs) {
                IntValueKV index = fieldIndex2RowIndex.get(indexName);
                if (index == null) {
                    synchronized (this) {
                        index = fieldIndex2RowIndex.get(indexName);
                        if (index == null) {
                            index = new IntValueKV(tableCompress.getRowCount());
                            fieldIndex2RowIndex.put(indexName, index);
                        }
                    }

                }
                String[] nameIndexs = indexName.split(";");
                Arrays.sort(nameIndexs);
                Map<String, String> cacheValues = createRow(it.next());
                String indexValue = createIndexValue(cacheValues, nameIndexs);
                index.put(indexValue, i);
            }
            i++;
        }
        LOG.info(" finish poll data , the row count  is " + i + ". byte is " + tableCompress
            .getByteCount());
    }

    /**
     * 从table compress 中取出所有的行，构建索引。把
     *
     * @param dataCacheVar
     * @param fieldIndex2RowIndex
     */
    protected void buildIndex(CompressTable dataCacheVar, Map<String, Map<String, List<Integer>>> fieldIndex2RowIndex) {
        Iterator<Map<String, Object>> it = dataCacheVar.newIterator();
        int i = 0;
        while (it.hasNext()) {
            Map<String, Object> row = it.next();
            Map<String, String> cacheValues = createRow(row);
            putValue2Index(cacheValues, i, fieldIndex2RowIndex);
            i++;
        }

        LOG.info(" finish poll data , the row count  is " + i + ". byte is " + dataCacheVar
            .getByteCount());

    }

    /**
     * 创建索引结构
     *
     * @param value
     * @param rowIndex
     * @param fieldIndex2RowIndex
     */
    protected void putValue2Index(Map<String, String> value, Integer rowIndex,
                                  Map<String, Map<String, List<Integer>>> fieldIndex2RowIndex) {
        if (indexs == null || indexs.size() == 0) {
            return;
        }
        for (String indexName : indexs) {
            Map<String, List<Integer>> name2RowIndexs = fieldIndex2RowIndex.get(indexName);
            if (name2RowIndexs == null) {
                synchronized (this) {
                    name2RowIndexs = fieldIndex2RowIndex.get(indexName);
                    if (name2RowIndexs == null) {
                        name2RowIndexs = new HashMap<>();
                        fieldIndex2RowIndex.put(indexName, name2RowIndexs);
                    }
                }

            }
            String[] nameIndexs = indexName.split(";");
            Arrays.sort(nameIndexs);
            addValue2Index(indexName, name2RowIndexs, rowIndex, value, nameIndexs);
        }
    }

    /**
     * 根据索引名称，把不同的索引值创建key，value放入索引缓存
     *
     * @param indexName      索引名称，多字段索引用；分割
     * @param name2RowIndexs 索引值，list<id>
     * @param rowIndex       行号
     * @param row
     */
    protected void addValue2Index(String indexName, Map<String, List<Integer>> name2RowIndexs, Integer rowIndex,
                                  Map<String, String> row, String[] nameIndexs) {
        String indexValue = createIndexValue(row, nameIndexs);
        addValue2Index(name2RowIndexs, rowIndex, indexValue);
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
        if (indexValues != null && indexValues.length > 0) {
            String indexValue = MapKeyUtil.createKey(indexValues);
            return indexValue;
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
        Iterator<Map.Entry<String, Object>> iterator = row.entrySet().iterator();
        //把数据value从object转化成string
        while (iterator.hasNext()) {
            Map.Entry<String, Object> entry = iterator.next();
            if (entry != null && entry.getValue() != null && entry.getKey() != null) {
                cacheValues.put(entry.getKey(), entry.getValue().toString());
            }
        }
        return cacheValues;
    }

    public static IntDataType INTDATATYPE = new IntDataType();

    /**
     * 把确定多索引值，和行号放入索引缓存
     *
     * @param name2RowIndexs
     * @param rowIndex
     * @param indexValue
     */
    protected void addValue2Index(Map<String, List<Integer>> name2RowIndexs, Integer rowIndex,
                                  String indexValue) {

        List<Integer> rowIndexs = name2RowIndexs.get(indexValue);
        if (rowIndexs == null) {
            rowIndexs = new ArrayList<>();
            name2RowIndexs.put(indexValue, rowIndexs);
        }
        rowIndexs.add(rowIndex);

    }

    public boolean isUnique() {
        return isUnique;
    }

    public void setUnique(boolean unique) {
        isUnique = unique;
    }

    public List<String> getIndexs() {
        return indexs;
    }

    public void setIndexs(List<String> indexs) {
        this.indexs = indexs;
    }

}
