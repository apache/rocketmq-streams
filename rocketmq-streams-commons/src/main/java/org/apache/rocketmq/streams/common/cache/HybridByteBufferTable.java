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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import org.apache.rocketmq.streams.common.cache.compress.AbstractMemoryTable;
import org.apache.rocketmq.streams.common.threadpool.ThreadPoolFactory;
import org.apache.rocketmq.streams.common.utils.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HybridByteBufferTable extends AbstractMemoryTable {

    private static final Logger logger = LoggerFactory.getLogger(HybridByteBufferTable.class);

    public static int PAGE_SIZE = 1 << 12; //4k

    protected static final ByteOrder order = ByteOrder.nativeOrder();

    static String jobName;

    static final String mappedFilePrefix = "/tmp/dipper_";
    static final int MAX_FILE_SIZE = 1 << 35; //32 * 1024 * 1024 * 1024 = 32G; //文件的最大长度
    static final long MAX_CACHE_SIZE = 1L << 35; // 32 * 1024 * 1024 * 1024 = 32G

    private transient long fileSizeLimit = -1;

    private transient long cacheSizeLimit = -1;

    //超过cache的部分,需要用内存映射方式读取
    private transient FileChannel channel;

    //row length固定位2个byte
    private transient ByteBuffer rowLengthBuffer = ByteBuffer.allocateDirect(2);

    private transient ByteBuffer buffer256 = ByteBuffer.allocateDirect(256);

    //cache的数据, cache会顺序写入
    protected transient List<byte[][]> cacheRows = new ArrayList<>();

    //当前cache的行标
    private transient int curCacheRowIndex = 0;

    //cache的字节数, 需要小于 cacheSize;
    protected transient long curCacheByteSize = 0;

    //总的字节数
    protected transient long curTotalByteSize = 0;

    //总的行数
    protected transient int curTotalRowCount;

    protected transient MemoryPageCache pageCache;

    protected transient ConcurrentLinkedQueue<MemoryPageCache> queue = new ConcurrentLinkedQueue<MemoryPageCache>();

    public ExecutorService executor;

    //总的列的数量
    protected int columnCount;

    protected transient volatile boolean isFinishWrite = false;

    public HybridByteBufferTable(String jobName) {
        this.jobName = jobName;
        try {
            createFile();
            channel = createChannel();
        } catch (IOException e) {
            e.printStackTrace();
        }
        executor = ThreadPoolFactory.createFixedThreadPool(1, HybridByteBufferTable.class.getName() + "-" + jobName);
        executor.submit(new WriteFileTask());
    }

    //todo file lock
    private final String createFile() throws IOException {
        String path = mappedFilePrefix + jobName;
        File file = new File(path);
        boolean isSuccess = file.exists();
        if (isSuccess) {
            return path;
        } else {
            isSuccess = file.createNewFile();
        }
        if (isSuccess) {
            return path;
        } else {
            logger.error(String.format("create mapped file error, file path is %s", path));
            return null;
        }
    }

    private final FileChannel createChannel() throws IOException {
        String filePath = createFile();
        RandomAccessFile raf = new RandomAccessFile(filePath, "rwd");
        FileChannel fc = raf.getChannel();
        return fc;
    }

    @Override public Iterator<RowElement> newIterator() {

        return new Iterator<RowElement>() {

            protected long nextCursor = 0;
            private final long totalByteCount = curTotalByteSize;
//            private long count = 0;

            @Override public boolean hasNext() {
//                check(getByteTotalSize());
                return nextCursor < totalByteCount;
            }

            @Override public RowElement next() {
//                check(getByteTotalSize());
                long current = nextCursor;
                Map<String, Object> row = new HashMap<>();
                try {
                    nextCursor = getRowAndNext(current, row);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                RowElement element = new RowElement(row, current);
                return element;
            }

            private final void check(long count) {
//                if(this.totalByteCount != count){
//                    throw new ConcurrentModificationException("unsupported modified. " + this.totalByteCount + " != " + count);
//                }
            }
        };
    }

    public Long saveRowByte(byte[][] values) {
        return saveRowByte(values, -1);
    }

    @Override protected Long saveRowByte(byte[][] values, int byteSize) {
        int size = 0;
        byte[][] byteList = new byte[values.length + 1][];
        int i = 0;
        for (byte[] bytes : values) {
            byte[] lenBytes = createColumnLenByte(bytes.length);
            byte[] columnBytes = new byte[lenBytes.length + bytes.length];
//            int index = 0;
            System.arraycopy(lenBytes, 0, columnBytes, 0, lenBytes.length);
            System.arraycopy(bytes, 0, columnBytes, lenBytes.length, bytes.length);
//            for(byte b : lenBytes){
//                columnBytes[index] = b;
//                index++;
//            }
//            for(byte b : bytes){
//                columnBytes[index] = b;
//                index++;
//            }
            byteList[++i] = columnBytes;
//            byteList.add(columnBytes);
            size += columnBytes.length;
        }
//        size = size + 2;
        //计算整行的长度
        byte[] rowLength = createRowLenByte(size);
//        int startIndex = currentIndex.getAndIncrement();
        //指向下一个
        byteList[0] = rowLength;//加上长度,把长度放到第一个位置
        size = size + rowLength.length;
        //写入成功之后再加size
        long ret = 0;
        try {
            ret = save(byteList, values, size);
        } catch (InterruptedException exception) {
            exception.printStackTrace();
        }

        return ret;
    }

    /**
     * 从文件中加载行
     *
     * @param index
     * @return
     */
    @Override protected byte[][] loadRowByte(Long index) {

//        boolean isCached = ((index & 0x8000000000000000L) == 0x8000000000000000L);
//        Long realIndex = index & 0x7fffffffffffffffL; //去掉第一位
        byte[][] bytes = null;
//        if(isCached){
//            bytes = this.cacheRows.get(realIndex.intValue());
//        }
        if (bytes == null) {
            try {
                getRowData(index, bytes);
            } catch (IOException e) {
                e.printStackTrace();
            }
//            byte[][] byteRows = new byte[bytes.size()][];
//            int i = 0;
//            for(byte[] byteArray : bytes){
//                byteRows[i] = byteArray;
//                i++;
//            }
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
        byte[] rowLenBytes = readRowLen(cursor);
        //计算整行的长度, 不包含行长度的元数据
        int length = readLenFromBytes(rowLenBytes);
        this.curTotalByteSize = this.curTotalByteSize + length + rowLenBytes.length;
        //解析出整行的数据
        byte[] bytes = readRowByteFromFile(length, cursor + rowLenBytes.length);
//        byte[][] datas = new byte[columnCount][];
        int index = 0;
        //遍历整行
        int i = 0;
        while (index < length) {
            byte[] lenBytes = readLen(bytes, index);//获取整行的byte
            index = index + lenBytes.length;
            int len = readLenFromBytes(lenBytes);//把列
            byte[] column = readByteFromBytes(bytes, index, len);
            data[i++] = column;
            index = index + column.length;
        }
        return length + rowLenBytes.length;
    }

    private byte[] readByteFromBytes(byte[] bytes, int index, int len) {
        byte[] ret = new byte[len];
        for (int i = 0; i < len; i++) {
            ret[i] = bytes[index++];
        }
        return ret;
    }

    /**
     * 从字节数组解析长度
     *
     * @param bytes
     * @return
     */
    private int readLenFromBytes(byte[] bytes) {
        int bytesLength = bytes.length;
        if (bytesLength == 1) {
            return NumberUtils.toInt(bytes[0]);
        } else {
            bytes[1] = (byte) (bytes[1] & 0x7f); //去掉第一位
            return NumberUtils.toInt(bytes);
        }
    }

    /**
     * 读取行的长度, 由于行的长度固定位2个的字节，使用固定的buffer
     *
     * @param offset
     * @return
     * @throws IOException
     */
    private byte[] readRowLen(long offset) throws IOException {
        return readByteFromChannel(this.rowLengthBuffer, offset);
    }

    private byte[] readByteFromChannel(ByteBuffer buffer, long offset) throws IOException {
        buffer.clear();
        channel.read(buffer, offset);
        buffer.flip();
        return buffer.array();
    }

    private byte[] readLen(byte[] bytes, int index) {
        byte b = bytes[index];
        //第一位为1
        if ((b & 0x80) == 0x80) {
            byte[] ret = new byte[2];
            ret[0] = b;
            ret[1] = bytes[++index];
            return ret;
        } else {
            byte[] ret = new byte[1];
            ret[0] = b;
            return ret;
        }
    }

    /**
     * 从文件中读取数据
     *
     * @param rowLength
     * @param offset
     * @return
     */
    private byte[] readRowByteFromFile(int rowLength, long offset) throws IOException {

        if (rowLength <= 256) {
            return this.readByteFromChannel(this.buffer256, offset);
        } else {
            ByteBuffer buffer = ByteBuffer.allocateDirect(rowLength);
            return this.readByteFromChannel(buffer, offset);

        }
    }

    /**
     * cache 缓存原始的数据, file写入带有meta的数据
     *
     * @param dataWithLenMeta 带有长度的byte数组
     * @param totalSize       原始的byte数组
     * @param totalSize       总的byte length
     * @throws IOException
     */
    //todo buildIndex
    private long save(byte[][] dataWithLenMeta, byte[][] rawData, int totalSize) throws InterruptedException {
//        MappedByteBuffer mappedByteBuffer = getOrCreateMappedByteBufferWithCapacity(byteSize);
//        if(byteSize > getFileSizeOrDefault()){
//            throw new RuntimeException("row is too long, size is " + byteSize);
//        }
        byte[] lenBytes = dataWithLenMeta[0];
        if ((lenBytes[1] & 0x80) == 0x80) {
            curCacheByteSize = curCacheByteSize + totalSize;
            curCacheRowIndex++;
            this.cacheRows.add(rawData);
        }
        curTotalByteSize = curTotalByteSize + totalSize;
        curTotalRowCount++;
        fillPage(dataWithLenMeta, totalSize);
        return curTotalByteSize;
    }

    /**
     * 计算整行的长度. 固定2个字节
     * 最高位为 1 代表 在cache中.
     * 最高位为 0 代表 在file中.
     * 行长度最高表示为 1 << 15 - 1 = 32767
     *
     * @param rowLength
     * @return
     */
    private byte[] createRowLenByte(int rowLength) {
        if (rowLength > 32767) {
            logger.error(String.format("row length[%d] exceeded max length 32767", rowLength));
            throw new RuntimeException(String.format("row length[%d] exceeded max length 32767", rowLength));
        }
        byte[] len = NumberUtils.toByteArray(rowLength, 0xffff);
        //如果为短列且cache有余量
        if (isOpenCached() && isShortLength(rowLength) && isCacheRemaining(rowLength)) {
            len[1] = (byte) (len[1] | 0x80);
        }
        return len;
    }

    /**
     * 创建row length的时候, 小于256的写入cache
     *
     * @param rowLength
     * @return
     */
    private final boolean isShortLength(int rowLength) {
        return rowLength <= 256;
    }

    /**
     * cache是否还有余量
     *
     * @return
     */
    private final boolean isCacheRemaining(int rowLength) {
        return (this.curCacheByteSize + rowLength) < this.cacheSizeLimit;
    }

    private final boolean getRowDataType(byte[] rowLength) {
        return (rowLength[1] & 0x80) == 0x80;
    }

    public final boolean isOpenCached() {
        return this.cacheSizeLimit > 0;
    }

    /**
     * 创建列的长度, 对于小于127的列, 用1个byte表示, 对于大于等于128 小于 等于32767的列,用两个字节表示
     *
     * @param columnSize
     * @return
     */
    private byte[] createColumnLenByte(int columnSize) {
        byte[] ret;
        int length = columnSize;
        //max length 65535/2
        if (length >= 0 && length <= 127) {
            ret = NumberUtils.toByteArray(length, 0xff);
        } else if (length >= 128 && length <= 32767) {
            ret = NumberUtils.toByteArray(length, 0xffff);
            //第一位置成1
            ret[1] = (byte) ((ret[1]) | ((byte) 0x80));
        } else {
            throw new RuntimeException("length must be 1 ~ 32767, but " + columnSize);
        }
        return ret;
    }

    private void fillPage(byte[][] data, int totalSize) throws InterruptedException {
        if (pageCache == null) {
            pageCache = new MemoryPageCache();
        }
        byte[] d = new byte[totalSize];
        int index = 0;
        for (byte[] b : data) {
            System.arraycopy(b, 0, d, index, b.length);
            index = index + b.length;
        }
        int pos = pageCache.filling(d, 0);
        //pos大于0, 表示当前page已经写满, 需要添加到q, 然后创建下一个
        boolean blocking = false;
        while (pos > 0) {
            MemoryPageCache clone = pageCache.deepCopy();
            if (queue == null) {
                queue = new ConcurrentLinkedQueue<>();
            }
            blocking = queue.offer(clone);
            if (blocking) {
                Thread.sleep(50);
            }
            pageCache = new MemoryPageCache();
            pos = pageCache.filling(d, pos);
        }
    }

    public String getJobName() {
        return jobName;
    }

    /**
     * @param index
     * @param rowMap
     * @return 返回下一个迭代的offset
     */
    public long getRowAndNext(long index, Map<String, Object> rowMap) throws IOException {
        byte[][] bytes = new byte[columnCount][];
        long readLength = getRowData(index, bytes);
        Map<String, Object> map = this.byte2Row(bytes);
        rowMap.putAll(map);
        return index + readLength;
    }

    public int getColumnCount() {
        return columnCount;
    }

    public void setColumnCount(int columnCount) {
        this.columnCount = columnCount;
    }

    public boolean isFinishWrite() {
        return isFinishWrite;
    }

    public void setFinishWrite(boolean finishWrite) {
        isFinishWrite = finishWrite;
    }

    public int getQueueSize() {
        return queue.size();
    }

    public static class FilePosition implements Serializable {

        private static final long serialVersionUID = 9128119471875329716L;

        //表示在第几个file里
        public int fileIndex;
        //表示当前mappedbytebuffer的position
        public long bufferPosition;

        public long globalCursor;

        public FilePosition(int a, long b) {
            fileIndex = a;
            bufferPosition = b;
        }

        public int getFileIndex() {
            return fileIndex;
        }

        public void setFileIndex(int fileIndex) {
            this.fileIndex = fileIndex;
        }

        public long getBufferPosition() {
            return bufferPosition;
        }

        public void setBufferPosition(long bufferPosition) {
            this.bufferPosition = bufferPosition;
        }

        public long getGlobalCursor() {
            return globalCursor;
        }

        public void setGlobalCursor(long globalCursor) {
            this.globalCursor = globalCursor;
        }
    }

    public static class MemoryPageCache {

        static int pageCount = -1;

        ByteBuffer page = ByteBuffer.allocateDirect(16);
        int length;
        long positionAtFile; //写入文件的位置

        MemoryPageCache() {
            this(false);
        }

        MemoryPageCache(boolean isCopy) {
            if (!isCopy) {
                pageCount++;
            }
            positionAtFile = pageCount * PAGE_SIZE;
//            System.out.println("current pageCount is " + pageCount);
        }

        /**
         * @param data
         * @return 如果未写满, 返回0, 如果当前page cache已经写满, 返回数组中间的位置。
         * @parm arrayPos 从数组的什么位置开始写入
         */
        public int filling(byte[] data, int arrayPos) {
            if (data.length == 0) {
                return -1;
            }
            int left = page.limit() - page.position();
            int destLength = data.length - arrayPos;
            int nextArrayPos = 0;
            if (left >= destLength) {
                page.put(data, arrayPos, destLength);
                length += destLength;
                nextArrayPos = arrayPos + data.length;
            } else {
                page.put(data, arrayPos, left);
                length += left;
                nextArrayPos = arrayPos + left;
//                page.flip();
            }
            return nextArrayPos;
        }

        public MemoryPageCache deepCopy() {
            MemoryPageCache cache = new MemoryPageCache(true);
            cache.page.put(this.page);
            cache.positionAtFile = this.positionAtFile;
            cache.length = this.length;
            return cache;
        }

    }

    public class WriteFileTask implements Runnable {

        @Override public void run() {
            MemoryPageCache cache = null;
            double count = 0.0;
            while (true) {
                cache = queue.poll();
                if (cache == null) {
                    if (isFinishWrite()) {
                        break;
                    }
                    try {
                        Thread.sleep(50);
                        continue;
                    } catch (InterruptedException exception) {
                        exception.printStackTrace();
                    }
                }
                try {
                    cache.page.flip();
                    channel.write(cache.page);
                    count += cache.page.limit();

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            try {
                if (pageCache != null) {
                    pageCache.page.flip();
                    channel.write(pageCache.page);
                    count += pageCache.page.limit();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("write finish : " + count / (1 << 20));
        }

    }

}