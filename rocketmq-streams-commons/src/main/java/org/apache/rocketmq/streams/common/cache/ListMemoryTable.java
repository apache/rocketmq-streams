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

/**
 * 压缩表，行数据以byte[][]存放
 */
public class ListMemoryTable extends AbstractMemoryTable {

    /**
     * /**
     * 表的全部数据，一行是一个byte[][]
     */
    protected List<byte[][]> rows = new ArrayList<>();

    /**
     * 保存row到list
     *
     * @param values
     * @return
     */
    @Override
    protected Long saveRowByte(byte[][] values, int byteSize) {
        this.rows.add(values);
        return Long.valueOf(rows.size() - 1);
    }

    /**
     * 从list中加载行
     *
     * @param index
     * @return
     */
    @Override
    protected byte[][] loadRowByte(Long index) {
        return this.rows.get(index.intValue());
    }

    /**
     * 创建迭代器，可以循环获取全部数据，一行数据是Map<String, Object>
     *
     * @return
     */
    @Override
    public Iterator<RowElement> newIterator() {
        return new Iterator<RowElement>() {

            protected long rowIndex = 0;

            @Override
            public boolean hasNext() {
                return rowIndex < getRowCount();
            }

            @Override
            public RowElement next() {
                Map<String, Object> row = getRow(rowIndex);
                return new RowElement(row, rowIndex++);
            }
        };
    }

}
