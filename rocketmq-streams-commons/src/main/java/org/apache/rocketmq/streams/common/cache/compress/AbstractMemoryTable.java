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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.datatype.NotSupportDataType;
import org.apache.rocketmq.streams.common.datatype.StringDataType;
import org.apache.rocketmq.streams.common.utils.DataTypeUtil;
import org.apache.rocketmq.streams.common.utils.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 压缩表，行数据以byte[][]存放
 */
public abstract class AbstractMemoryTable {

    private static final Logger logger = LoggerFactory.getLogger(AbstractMemoryTable.class);

    /**
     * 列名和位置，根据列名生成一个位置
     */
    protected Map<String, Integer> cloumnName2Index = new HashMap<>();

    /**
     * 位置和列名，根据位置获取列名
     */
    protected Map<Integer, String> index2ColumnName = new HashMap<>();

    /**
     * 列名和类型的映射关系
     */
    protected Map<String, DataType> cloumnName2DatatType = new HashMap<>();

    /**
     * 列名当前索引值。根据获取的列的先后顺序，建立序号
     */
    protected AtomicInteger index = new AtomicInteger(0);

    /**
     * 总字节数
     */
    protected AtomicLong byteCount = new AtomicLong(0);
    protected AtomicInteger rowCount = new AtomicInteger(0);

    //需要压缩的列名
    Set<String> compressFieldNames = new HashSet<>(1);

    public Set<String> getCompressFieldNames() {
        return compressFieldNames;
    }

    public void setCompressFieldNames(Set<String> compressFieldNames) {
        this.compressFieldNames = compressFieldNames;
    }

    static long compressCount = 0;
    transient long compressCompareCount = 0;
    transient long compressByteLength = 0;
    transient long deCompressByteLength = 0;

    public static class RowElement {
        protected Map<String, Object> row;
        protected Long rowIndex;

        public RowElement(Map<String, Object> row, Long rowIndex) {
            this.row = row;
            this.rowIndex = rowIndex;
        }

        public Map<String, Object> getRow() {
            return row;
        }

        public long getRowIndex() {
            return rowIndex;
        }

        @Override
        public String toString() {
            return "RowElement{" +
                "row=" + row +
                ", rowIndex=" + rowIndex +
                '}';
        }
    }

    /**
     * 创建迭代器，可以循环获取全部数据，一行数据是Map<String, Object>
     *
     * @return
     */
    public abstract Iterator<RowElement> newIterator();

    public Iterator<Map<String, Object>> rowIterator() {
        return new Iterator<Map<String, Object>>() {
            Iterator<RowElement> it = newIterator();

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public Map<String, Object> next() {
                RowElement rowElement = it.next();
                return rowElement.getRow();
            }
        };
    }

    /**
     * 保存row到list
     *
     * @param values
     * @return
     */
    protected abstract Long saveRowByte(byte[][] values, int byteSize);

    /**
     * 从list中加载行
     *
     * @param index
     * @return
     */
    protected abstract byte[][] loadRowByte(Long index);

    /**
     * 增加一行数据，会进行序列化成二进制数组存储。数字类型会有较好的压缩效果，字符串未做压缩
     *
     * @param row
     * @return
     */
    public Long addRow(Map<String, Object> row) {
        CountHolder countHolder = new CountHolder();
        byte[][] values = row2Byte(row, countHolder);
        byteCount.addAndGet(countHolder.count);
        rowCount.incrementAndGet();
        return saveRowByte(values, countHolder.count);
    }

    /**
     * 获取一行数据，会反序列化为Map<String, Object>
     *
     * @param index
     * @return
     */
    public Map<String, Object> getRow(Long index) {
        byte[][] bytes = loadRowByte(index);
        if (bytes == null) {
            return null;
        }
        return byte2Row(bytes);
    }

    /**
     * 把一个row行转换成byte[][]数组
     *
     * @param row
     * @return
     */
    public byte[][] row2Byte(Map<String, Object> row) {
        return row2Byte(row, null);
    }

    protected class CountHolder {
        int count = 0;
    }

    /**
     * 把一个row行转换成byte[][]数组
     *
     * @param row
     * @return
     */
    protected byte[][] row2Byte(Map<String, Object> row, CountHolder countHolder) {

        //        byte[][] byteRows=new byte[row.size()][];
        List<byte[]> list = new ArrayList<>();
        Iterator<Map.Entry<String, Object>> it = row.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Object> entry = it.next();
            String fieldName = entry.getKey();
            int index = getColumnIndex(entry.getKey());
            byte[] byteValue = createBytes(entry.getKey(), entry.getValue());
            int byteLength = 0;
            if (byteValue == null) {
                list.add(new byte[0]);
                //                byteRows[value]=new byte[0];
            } else {
                //todo compress string type
                if (compressFieldNames.contains(fieldName)) {
                    byte[] compressValue = NumberUtils.zlibCompress(byteValue);
                    list.add(compressValue);
                    byteLength = compressValue.length;
                    if (byteLength >= byteValue.length) {
                        compressCompareCount++;
                        deCompressByteLength = deCompressByteLength + compressValue.length - byteValue.length;
                    }
                    compressByteLength = compressByteLength + byteValue.length - compressValue.length;
                } else {
                    list.add(byteValue);
                    byteLength = byteValue.length;
                }
            }
            if (countHolder != null) {
                countHolder.count = countHolder.count + byteLength;
            }
        }
        byte[][] byteRows = new byte[list.size()][];
        for (int i = 0; i < list.size(); i++) {
            byte[] temp = list.get(i);
            byteRows[i] = temp;
        }
        if ((compressCount++) % 100000 == 0) {
            logger.info(this.hashCode() + " builder compress table continue..." + (compressCount - 1) + ", compressCompareCount is " + compressCompareCount + ", compressByteLength is " + compressByteLength + ", deCompressByteLength is " + deCompressByteLength);
        }
        return byteRows;
    }

    /**
     * 把序列化的字节数组转换成map
     *
     * @param bytes 一行的字节数组
     * @return
     */
    public Map<String, Object> byte2Row(byte[][] bytes) {
        Map<String, Object> row = new HashMap<>();
        for (int i = 0; i < bytes.length; i++) {
            byte[] columnValue = bytes[i];
            String columnName = index2ColumnName.get(i);
            boolean isCompress = compressFieldNames.contains(columnName);
            DataType dataType = cloumnName2DatatType.get(columnName);
            Object object = null;

            if (isCompress) {
                byte[] decompressValue = null;
                try {
                    decompressValue = NumberUtils.zlibInfCompress(columnValue);
                    object = dataType.byteToValue(decompressValue);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                object = dataType.byteToValue(columnValue);
            }

            row.put(columnName, object);
        }
        return row;
    }

    /**
     * 把数据转换成字节
     *
     * @param key
     * @param value
     * @return
     */
    private byte[] createBytes(String key, Object value) {
        if (value == null) {
            return null;
        }
        Object tmp = value;
        DataType dataType = cloumnName2DatatType.get(key);
        if (dataType == null) {
            dataType = DataTypeUtil.getDataTypeFromClass(value.getClass());
            if (dataType == null || dataType.getClass().getName().equals(NotSupportDataType.class.getName())) {
                dataType = new StringDataType();
                tmp = value.toString();
            }
            cloumnName2DatatType.put(key, dataType);
        }
        Object object = dataType.convert(tmp);
        return dataType.toBytes(object, true);
    }

    /**
     * 给每个列一个index，方便把数据放入数组中
     *
     * @param key
     * @return
     */
    public int getColumnIndex(String key) {
        Integer columnIndex = cloumnName2Index.get(key);
        if (columnIndex == null) {
            columnIndex = index.incrementAndGet() - 1;
            cloumnName2Index.put(key, columnIndex);
            index2ColumnName.put(columnIndex, key);
        }
        return columnIndex;
    }

    public Map<String, Integer> getCloumnName2Index() {
        return cloumnName2Index;
    }

    public Map<Integer, String> getIndex2ColumnName() {
        return index2ColumnName;
    }

    public Map<String, DataType> getCloumnName2DatatType() {
        return cloumnName2DatatType;
    }

    public void setCloumnName2Index(Map<String, Integer> cloumnName2Index) {
        this.cloumnName2Index = cloumnName2Index;
    }

    public void setIndex2ColumnName(Map<Integer, String> index2ColumnName) {
        this.index2ColumnName = index2ColumnName;
    }

    public void setCloumnName2DatatType(Map<String, DataType> cloumnName2DatatType) {
        this.cloumnName2DatatType = cloumnName2DatatType;
    }

    public long getByteCount() {
        return byteCount.get();
    }

    public int getRowCount() {
        return rowCount.get();
    }

}
