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
package org.apache.rocketmq.streams.common.cache;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.rocketmq.streams.common.cache.compress.AbstractMemoryTable;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.utils.DataTypeUtil;
import org.apache.rocketmq.streams.common.utils.NumberUtils;

/**
 * @program rocketmq-streams-apache
 * @description
 */
public abstract class FileBasedTable extends AbstractMemoryTable {

    protected static final ByteOrder order = ByteOrder.nativeOrder();
    static transient String mappedFilePrefix = "/tmp/dipper_";
    protected transient long fileOffset = 0; // unit -> byte
    protected transient int fileRowCount = 0; //总的行数
    String fileName;
    int columnsCount = -1;
    TableSchema tableSchema;

    protected FileBasedTable() {

    }

    public FileBasedTable(String fileName, int columnsCount) {
        this(fileName, columnsCount, null);
    }

    public FileBasedTable(String fileName, int columnsCount, TableSchema schema) {
        this.fileName = fileName;
        this.columnsCount = columnsCount;
        this.tableSchema = schema;
        if (schema != null) {
            for (int i = 0; i < schema.schemaLength(); i++) {
                String fieldName = tableSchema.getField(i);
                String fieldType = tableSchema.getFieldType(i);
                DataType dataType = DataTypeUtil.getDataType(fieldType);
                columnName2Index.put(fieldName, i);
                index2ColumnName.put(i, fieldName);
                columnName2DataType.put(fieldName, dataType);
            }
        }

    }

    public static String createRealFilePath(String fileName, String cyclePeriod, String index) {
        return mappedFilePrefix + fileName + "_" + cyclePeriod + "_" + index;
    }

    public static String createLockFilePath(String fileName, String cyclePeriod) {
        return mappedFilePrefix + fileName + "_" + cyclePeriod + "_lock";
    }

    public static String createDoneFilePath(String fileName, String cyclePeriod) {
        return mappedFilePrefix + fileName + "_" + cyclePeriod + "_done";
    }

    // row id
    @Override
    public Iterator<RowElement> newIterator() {

        return new Iterator<RowElement>() {

            private final long totalByteCount = fileOffset;
            protected long nextCursor = 0;

            @Override
            public boolean hasNext() {
                return nextCursor < totalByteCount;
            }

            @Override
            public RowElement next() {
                long current = nextCursor;
                Map<String, Object> row = new HashMap<>();
                try {
                    nextCursor = nextCursor + getRowAndNext(current, row);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                RowElement element = new RowElement(row, current);
                return element;
            }

            /**
             *
             * @param index
             * @param rowMap
             * @return 返回下一个迭代的offset
             */
            public long getRowAndNext(long index, Map<String, Object> rowMap) throws IOException {
                byte[][] bytes = new byte[columnsCount][];
                long readLength = getRowData(index, bytes);
                Map<String, Object> map = byte2Row(bytes);
                rowMap.putAll(map);
                return readLength;
            }

        };
    }

    /**
     * 保存row到文件，整个数据的构成：行长度（2个字节）+ 行字节数组
     * 每行多列：每一列前面加一个长度，标识这个列代表的字节数组的长度
     * 列长度用1-2个字节表示，根据第一位是0还是1，决定是1个字节还是两个字节
     * <p>
     * 首先需要为每列计算长度, 之后每列加上列长之后统一计算行的长度。
     * 列长不包含本身元数据的长度
     * 比如 列为 1,2,3,4,5。写入的byte为 05, 01, 02, 03, 04, 05。第一个byte为列byte数组长度。
     *
     * @param values
     * @return row id
     */
    @Override
    protected Long saveRowByte(byte[][] values, int byteSize) {
        int size = 0;
        byte[][] byteList = new byte[values.length + 1][];
        int i = 0;
        for (byte[] bytes : values) {
            byte[] lenBytes = columnLen2Byte(bytes.length);
            byte[] columnBytes = new byte[lenBytes.length + bytes.length];
            System.arraycopy(lenBytes, 0, columnBytes, 0, lenBytes.length);
            System.arraycopy(bytes, 0, columnBytes, lenBytes.length, bytes.length);
            byteList[++i] = columnBytes;
            size += columnBytes.length;
        }
        //计算整行的长度, 固定为2
        byte[] rowLength = rowLen2Byte(size);
//        int startIndex = currentIndex.getAndIncrement();
        //指向下一个
        byteList[0] = rowLength;//加上长度,把长度放到第一个位置
        size = size + rowLength.length;
        //写入成功之后再加size

        long ret = 0;

        try {
            ret = save(byteList, values, size, fileOffset);
        } catch (InterruptedException exception) {
            exception.printStackTrace();
        }

        if ((ret - size) != (fileOffset - size)) {
            System.err.println("error " + fileRowCount);
        }
        return fileOffset - size;
    }

    /**
     * 父类是通过内部index表示数据的顺序, 和写入的数据一致.这里需要保障row字段写入有序.
     *
     * @param row
     * @param countHolder
     * @return
     */
    @Override
    protected byte[][] row2Byte(Map<String, Object> row, CountHolder countHolder) {
        if (tableSchema != null) {
            Map<String, Object> linkedRow = new LinkedHashMap<>();
            for (int i = 0; i < tableSchema.schemaLength(); i++) {
                String key = tableSchema.getField(i);
                Object value = row.get(key);
                linkedRow.put(key, value);
            }
            return super.row2Byte(linkedRow, countHolder);
        }
        if (columnsCount == -1) {
            columnsCount = row.size();
        }
        return super.row2Byte(row, countHolder);
    }

    /**
     * 创建列的长度, 对于小于127的列, 用1个byte表示, 对于大于等于128 小于 等于32767的列,用两个字节表示
     *
     * @param columnSize
     * @return
     */
    private byte[] columnLen2Byte(int columnSize) {
        byte[] ret;
        int length = columnSize;
        //max length 65535/2
        if (length >= 0 && length <= 127) {
            ret = NumberUtils.toByteArray(length, 0xff);
        } else if (length >= 128 && length <= 32767) {
            ret = NumberUtils.toByteArray(length, 0xffff);
            //第一位置成1
            ret[1] = (byte) ((ret[1]) | ((byte) 0x80));
            return swap(ret);
        } else {
            throw new RuntimeException("length must be 1 ~ 32767, but " + columnSize);
        }
        return ret;
    }

    private byte[] swap(byte[] b) {
        byte[] ret = new byte[2];
        ret[1] = b[0];
        ret[0] = b[1];
        return ret;
    }

    /**
     * 计算行的长度, 默认为2
     *
     * @param rowLength
     * @return
     */
    byte[] rowLen2Byte(int rowLength) {
        byte[] len = NumberUtils.toByteArray(rowLength, 0xffff);
        return len;
    }

    /**
     * cache 缓存原始的数据, file写入带有meta的数据
     *
     * @param dataWithLenMeta 带有长度信息的byte数组(行长度, 列长度)
     * @param totalSize       原始的byte数组
     * @param totalSize       总的byte length
     * @return offset nextOffset
     * @throws IOException
     */
    private long save(byte[][] dataWithLenMeta, byte[][] rawData, int totalSize,
        long offset) throws InterruptedException {

        byte[] lenBytes = dataWithLenMeta[0];
        long writeLength = save2File(dataWithLenMeta, offset);
        if (writeLength > 0) {
            fileOffset = fileOffset + writeLength;
            fileRowCount++;
        }
        return offset + writeLength;
    }

    abstract long save2File(byte[][] data, long offset);

    @Override
    protected byte[][] loadRowByte(Long index) {

        byte[][] bytes = new byte[columnsCount][];
        try {
            getRowData(index, bytes);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return bytes;
    }

    /**
     * readIndex 为全局游标
     *
     * @param readIndex
     * @return 返回读取数据的字节长度
     * @throws IOException
     */
    protected long getRowData(long readIndex, byte[][] data) throws IOException {

        long cursor = readIndex;
        //解析整行的长度
        byte[] rowLenBytes = loadRowLengthByte(cursor);
        //计算整行的长度, 不包含行长度的元数据
        int length = rowLenByte2Int(rowLenBytes);
        //解析出整行的数据
        byte[] bytes = loadFromFile(cursor + rowLenBytes.length, length);
//        byte[][] datas = new byte[columnCount][];
        //遍历整行, 解析列
        int arrayIndex = 0;
        int i = 0;
        while (arrayIndex < length) {
            byte[] lenBytes = readColumnLen(bytes, arrayIndex);//获取列的长度byte
            arrayIndex = arrayIndex + lenBytes.length;
            int len = parseColumnLengthInt(lenBytes);//解析列的长度
            byte[] column = readByteFromBytes(bytes, arrayIndex, len);
            data[i++] = column;
            arrayIndex = arrayIndex + len;
        }
        return length + rowLenBytes.length;
    }

    private byte[] loadRowLengthByte(long offset) {
        return loadFromFile(offset, 2);
    }

//    int lengthByte2Int(byte[] bytes){
//        int bytesLength = bytes.length;
//        if(bytesLength == 1){
//            return NumberUtils.toInt(bytes[0]);
//        }else{
//            bytes[1] = (byte)(bytes[1] & 0x7f); //去掉第一位
//            return NumberUtils.toInt(bytes);
//        }
//    }

    abstract byte[] loadFromFile(long offset, int len);

    abstract boolean destroy();

    int rowLenByte2Int(byte[] bytes) {
        return NumberUtils.toInt(bytes);
    }

    private byte[] readColumnLen(byte[] bytes, int index) {
        byte b = bytes[index];
        //第一位为1
        if ((b & 0x80) == 0x80) {
            byte[] ret = new byte[2];
            ret[1] = (byte) (b & 0x7f);
            ret[0] = bytes[++index];
            return ret;
        } else {
            byte[] ret = new byte[1];
            ret[0] = b;
            return ret;
        }
    }

    private int parseColumnLengthInt(byte[] bytes) {
        int bytesLength = bytes.length;
        if (bytesLength == 1) {
            return NumberUtils.toInt(bytes[0]);
        } else {
            bytes[1] = (byte) (bytes[1] & 0x7f); //去掉第一位
            return NumberUtils.toInt(bytes);
        }
    }

    /**
     * 内部方法, 主要用于一维数组转换成二维数组
     *
     * @param srcBytes
     * @param srcIndex
     * @param len
     * @return
     */
    private byte[] readByteFromBytes(byte[] srcBytes, int srcIndex, int len) {
        byte[] ret = new byte[len];
        System.arraycopy(srcBytes, srcIndex, ret, 0, len);
        return ret;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public int getColumnsCount() {
        return columnsCount;
    }

    public void setColumnsCount(int columnsCount) {
        this.columnsCount = columnsCount;
    }

    public long getFileOffset() {
        return fileOffset;
    }

    public void setFileOffset(long fileOffset) {
        this.fileOffset = fileOffset;
    }

    public int getFileRowCount() {
        return fileRowCount;
    }

    public void setFileRowCount(int fileRowCount) {
        this.fileRowCount = fileRowCount;
    }

    public TableSchema getTableSchema() {
        return tableSchema;
    }

    public void setTableSchema(TableSchema tableSchema) {
        this.tableSchema = tableSchema;
    }
}
