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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.cache.compress.AbstractMemoryTable;
import org.apache.rocketmq.streams.common.cache.compress.ByteStore;
import org.apache.rocketmq.streams.common.cache.compress.KVAddress;
import org.apache.rocketmq.streams.common.utils.NumberUtils;

public class ByteArrayMemoryTable extends AbstractMemoryTable {
    protected ByteStore cache = new ByteStore(-1);

    @Override public Iterator<RowElement> newIterator() {
        return new Iterator<RowElement>() {
            Iterator<ByteStore.DataElement> rows = cache.iterator();

            @Override public boolean hasNext() {
                return rows.hasNext();
            }

            @Override public RowElement next() {
                ByteStore.DataElement dataElement = rows.next();
                byte[][] row = createColumn(dataElement.getBytes());
                if (row == null) {
                    return null;
                }
                Map<String, Object> data = byte2Row(row);
                ;
                KVAddress mapAddress = dataElement.getMapAddress();
                long rowIndex = mapAddress.convertLongValue();
                return new RowElement(data, rowIndex);
            }
        };
    }

    @Override protected Long saveRowByte(byte[][] values, int byteSize) {
        byte[] row = createRowByte(values);
        KVAddress address = cache.add2Store(row);
        return address.convertLongValue();
    }

    @Override protected byte[][] loadRowByte(Long address) {
        KVAddress mapAddress = KVAddress.createMapAddressFromLongValue(address);
        byte[] rowByte = cache.getValue(mapAddress).getByteArray();
        byte[][] columns = createColumn(rowByte);
        return columns;
    }

    protected byte[] createRowByte(byte[][] values) {
        List<byte[]> list = new ArrayList<>();
        int byteSize = 0;
        for (byte[] bytes : values) {
            byte[] lenBytes = createLenBytes(bytes.length);
            byte[] column = new byte[lenBytes.length + bytes.length];
            for (int i = 0; i < lenBytes.length; i++) {
                column[i] = lenBytes[i];
            }
            for (int i = 0; i < bytes.length; i++) {
                column[i + lenBytes.length] = bytes[i];
            }
            byteSize = byteSize + column.length;
            list.add(column);
        }
        int i = 0;
        byte[] row = new byte[byteSize];
        for (byte[] column : list) {
            for (byte b : column) {
                row[i] = b;
                i++;
            }
        }
        return row;
    }

    protected byte[][] createColumn(byte[] row) {
        int index = 0;
        List<byte[]> list = new ArrayList<>();
        while (index < row.length) {
            byte[] lenBytes = getLenBytesFromRow(row, index);
            int len = NumberUtils.toInt(lenBytes);
            index = index + lenBytes.length;
            byte[] column = getByteFromRow(row, index, len);
            list.add(column);
            index = index + column.length;
        }
        byte[][] result = new byte[list.size()][];
        for (int i = 0; i < list.size(); i++) {
            result[i] = list.get(i);
        }
        return result;
    }

    private byte[] getLenBytesFromRow(byte[] row, int index) {
        return new byte[] {row[index], row[index + 1]};
    }

    private byte[] createLenBytes(int length) {
        byte[] value = NumberUtils.toByte(length);
        return new byte[] {value[0], value[1]};
    }

    private byte[] getByteFromRow(byte[] row, int index, int len) {
        byte[] result = new byte[len];
        for (int i = 0; i < len; i++) {
            result[i] = row[index + i];
        }
        return result;
    }

    public ByteStore getCache() {
        return cache;
    }
}
