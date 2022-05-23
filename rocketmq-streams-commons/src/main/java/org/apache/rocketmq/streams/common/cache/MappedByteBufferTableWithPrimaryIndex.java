///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package org.apache.rocketmq.streams.common.cache;
//
//import java.io.File;
//import java.io.IOException;
//import java.io.RandomAccessFile;
//import java.io.Serializable;
//import java.nio.ByteOrder;
//import java.nio.MappedByteBuffer;
//import java.nio.channels.FileChannel;
//import java.util.*;
//import java.util.concurrent.atomic.AtomicInteger;
//
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.apache.rocketmq.streams.common.cache.compress.AbstractMemoryTable;
//import org.apache.rocketmq.streams.common.utils.NumberUtils;
//
//@Deprecated
//public class MappedByteBufferTableWithPrimaryIndex extends AbstractMemoryTable {
//
//    private static final Log logger = LogFactory.getLog(MappedByteBufferTableWithPrimaryIndex.class);
//
//    protected String jobName;
//
//    protected static final String mappedFilePrefix = "/tmp/dipper_";
//
//    protected static final int SIZE = 1024 * 1024 * 1024;
//
//    protected int fileSize = -1;
//
//    //1g 1个文件，超过1g，可以多个文件，每个文件对应一个MappedByteBuffer
//    protected transient List<MappedByteBuffer> caches = new ArrayList<>();
//
//    //当一个文件写满时，把文件的索引号和起始位置map起来，方便查找
//    //key = rowId, value为全局索引号, 把二维索引变成一维索引
//    protected transient Map<Integer, Long> maxOffsets = new HashMap<>();
//
//    //row id
//    protected transient AtomicInteger currentIndex = new AtomicInteger(0);
//
//    //全局游标
//    protected transient volatile long cursor = 0;
//
//    protected transient volatile int currentFileIndex = 0;
//
//    private static final int MAX_FILE_COUNT = 2;
//
//    protected long totalMargin = 0;
//
//    protected static final ByteOrder order = ByteOrder.nativeOrder();
//
//    public MappedByteBufferTableWithPrimaryIndex(){
//
//    }
//
//    public MappedByteBufferTableWithPrimaryIndex(String jobName){
//        this.jobName = jobName;
//        this.fileSize = SIZE;
//    }
//
//    public MappedByteBufferTableWithPrimaryIndex(String jobName, int fileSize){
//        this.jobName = jobName;
//        this.fileSize = fileSize;
//    }
//
//
//    //todo file lock
//    private final String createMappedFile(int fileIndex) throws IOException {
//        String path = mappedFilePrefix + jobName + "_" + fileIndex;
//        File file = new File(path);
//        boolean isSuccess = file.exists();
//        if(isSuccess){
//            return path;
//        }else{
//            isSuccess = file.createNewFile();
//        }
//        if(isSuccess){
//            return path;
//        }else{
//            logger.error(String.format("create mapped file error, file path is %s", path));
//            return null;
//        }
//    }
//
//    private final MappedByteBuffer createMappedByteBuffer(int fileIndex) throws IOException {
//        String filePath = createMappedFile(fileIndex);
//        RandomAccessFile raf = new RandomAccessFile(filePath, "rw");
//        FileChannel fc = raf.getChannel();
//        MappedByteBuffer mbb = fc.map(FileChannel.MapMode.READ_WRITE, 0, getFileSizeOrDefault());
//        return mbb;
//    }
//
//    private final MappedByteBuffer getOrCreateMappedByteBuffer(int index) throws IOException {
//        MappedByteBuffer buffer = null;
//        if(index < caches.size()){
//            buffer = caches.get(index);
//        }
//        if(buffer == null){
//            buffer = createMappedByteBuffer(index);
//            caches.add(buffer);
//        }
//        return buffer;
//    }
//
//    private final int calCurrentFileIndex(long cursor){
//        return Long.valueOf((cursor/getFileSizeOrDefault())).intValue();
//    }
//
//    private final int calCurrentFileIndex(){
//        return calCurrentFileIndex(cursor);
//    }
//
//    private final FilePosition seek(long startIndex){
//
//        int fileIndex = Long.valueOf(startIndex / getFileSizeOrDefault()).intValue();
//        long bufferPosition = startIndex % getFileSizeOrDefault();
//        return new FilePosition(fileIndex, bufferPosition);
//
//    }
//
//// 1. 在 tmp目录创建文件，一个文件1g，最大4g(2g?)
//// 2. 一台物理机/虚拟机可能运行多个taskmanager，一个taskmanager可能会有多个并发，文件目录 为 tmp/${jobName}/00 ~ 03
//// 3. 初始化加写锁？
//// 4. index是什么意思？
//
//    @Override
//    public Iterator<RowElement> newIterator() {
//        return new Iterator<RowElement>() {
//
//            protected int rowIndex = 0;
//            private final int count = maxOffsets.size();
//
//            @Override
//            public boolean hasNext() {
//                check(maxOffsets.size());
//                return rowIndex < count;
//            }
//
//            @Override
//            public RowElement next() {
//                check(maxOffsets.size());
//                Map<String, Object> row = getRow(rowIndex);
//                return new RowElement(row, rowIndex++);
//            }
//
//            private final void check(int count){
//                if(this.count != count){
//                    throw new ConcurrentModificationException("unsupported modified. " + this.count + " != " + count);
//                }
//            }
//        };
//    }
//
//    /**
//     * 保存row到文件，整个数据的构成：行长度（2个字节）+ 行字节数组
//     * 每行多列：每一列前面加一个长度，标识这个列代表的字节数组的长度
//     * 长度用1-2个字节表示，根据第一位是0还是1，决定是1个字节还是两个字节
//     *
//     * @param values
//     * @return row id
//     */
//    @Override
//    protected Integer saveRowByte(byte[][] values, int byteSize) {
//        int size = 0;
//        List<byte[]> byteList = new ArrayList();
//        for(byte[] bytes : values){
//            byte[] lenBytes = createLenByte(bytes);
//            byte[] columnBytes = new byte[lenBytes.length + bytes.length];
//            int index = 0;
//            for(byte b : lenBytes){
//                columnBytes[index] = b;
//                index++;
//            }
//            for(byte b : bytes){
//                columnBytes[index] = b;
//                index++;
//            }
//            byteList.add(columnBytes);
//            size += columnBytes.length;
//        }
////        size = size + 2;
//        //计算整行的长度
//        byte[] rowLength = createLenByte(size);
//        int startIndex = currentIndex.getAndIncrement();
//        //指向下一个
//        byteList.add(0, rowLength);//加上长度
//        size = size + rowLength.length;
//        try {
//            //写入成功之后再加size
//            save2File(startIndex, byteList, size);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return startIndex;
//    }
//
//    /**
//     * 从文件中加载行
//     *
//     * @param startIndex
//     * @return
//     */
//    @Override
//    protected byte[][] loadRowByte(Integer startIndex) {
//
//        List<byte[]> bytes = null;
//        try {
//            bytes = getFromFile(startIndex);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        byte[][] byteRows = new byte[bytes.size()][];
//        int i = 0;
//        for(byte[] byteArray : bytes){
//            byteRows[i] = byteArray;
//            i++;
//        }
//        return byteRows;
//
//    }
//
//    /**
//     * startIndex 为row id
//     * @param startIndex
//     * @return
//     * @throws IOException
//     */
//    protected List<byte[]> getFromFile(int startIndex) throws IOException {
//
//        long cursor = maxOffsets.get(startIndex);
//        FilePosition position = seek(cursor);
//        int fileIndex = position.fileIndex;
//        long bufferCursor = position.bufferPosition;
//        MappedByteBuffer buffer = caches.get(fileIndex);
//        //解析整行的长度
//        byte[] rowLenBytes = readLen((int)bufferCursor, buffer);
//        //计算整行的长度, 不包含行长度的元数据
//        int length = readLenFromBytes(rowLenBytes);
//        //解析出整行的数据
//        byte[] bytes = readByteFromFile(buffer, bufferCursor + rowLenBytes.length, length);
//        List<byte[]>result = new ArrayList<>();
//        int index = 0;
//        while (index < length){
//            byte[] lenBytes = readLen(bytes, index);
//            index = index + lenBytes.length;
//            int len = readLenFromBytes(lenBytes);
//            byte[] column = readByteFromBytes(bytes, index, len);
//            result.add(column);
//            index = index + column.length;
//        }
//        return result;
//    }
//
//
//    private byte[] readByteFromBytes(byte[] bytes,int index, int len){
////        if(len == 0){
////            return new byte[0];
////        }
//        byte[] ret = new byte[len];
//        for(int i = 0; i < len; i++){
//            ret[i] = bytes[index++];
//        }
//        return ret;
//    }
//
//    /**
//     * 从字节数组解析长度
//     * @param bytes
//     * @return
//     */
//    private int readLenFromBytes(byte[] bytes){
//        int bytesLength = bytes.length;
//        assert (bytesLength == 1 || bytesLength == 2) : "bytes length must be 1 or 2, but " + Arrays.toString(bytes);
//        if(bytesLength == 1){
//            return NumberUtils.toInt(bytes[0]);
//        }else{
//            bytes[1] = (byte)(bytes[1] & 0x7f);
//            return NumberUtils.toInt(bytes);
//        }
//    }
//
//    private byte[] readLen(int start, MappedByteBuffer buffer){
//        buffer.position(0);
//        byte b = buffer.get(start);
//        int length = 1;
//        if((b & 0x80) == 0x80){
//            length = 2;
//        }
//        byte[] ret = new byte[length];
//        ret[0] = b;
//        if(length == 2){
//            ret[1] = buffer.get();
//        }
////        buffer.position(0);
////        System.out.println("start - " + start + ", " + "remaing - " + buffer.remaining() + ", " + "position - " + buffer.position());
////        buffer.get(ret, start, length);
//        return ret;
//    }
//
//    private byte[] readLen(byte[] bytes, int index) {
//        byte b = bytes[index];
//        //第一位为1
//        if((b & 0x80) == 0x80){
//            byte[] ret = new byte[2];
//            ret[0] = b;
//            ret[1] = bytes[++index];
//            return ret;
//        }else{
//            byte[] ret = new byte[1];
//            ret[0] = b;
//            return ret;
//        }
//    }
//
//    private byte[] readByteFromFile(MappedByteBuffer buffer, long startIndex, int len) throws IOException {
//        byte[] dst = new byte[len];
//        int index = Long.valueOf(startIndex).intValue();
//        buffer.position(index);
//        buffer.get(dst);
//        return dst;
//    }
//
//    /**
//     * startIndex 为row id
//     * @param startIndex
//     * @param list
//     * @param byteSize
//     * @throws IOException
//     */
//    private void save2File(int startIndex, List<byte[]> list, int byteSize) throws IOException {
//        MappedByteBuffer mappedByteBuffer = getOrCreateMappedByteBufferWithCapacity(byteSize);
//        if(byteSize > getFileSizeOrDefault()){
//            throw new RuntimeException("row is too long, size is " + byteSize);
//        }
//        for(byte[] bytes : list){
//            mappedByteBuffer.put(bytes);
//        }
//        maxOffsets.put(startIndex, cursor);
////        if((maxOffsets.size() % 100000) == 1){
////            System.out.println(maxOffsets.size());
////        }
//        cursor = cursor + byteSize;
//    }
//
//    /**
//     * 获取数组的长度, 大于等于128的用两个byte表示, 小于等于127的用一个byte表示
//     * @param bytes
//     * @return
//     */
//    private byte[] createLenByte(byte[] bytes) {
//        //todo 如果长度小于127，用一个字节表示长度，第一位为0
//        //如果长度大于127，用两个字节表示长度，第一位为1
//        //返回标识长度的字节数组
//        return createLenByte(bytes.length);
//    }
//
//    private byte[] createLenByte(int size) {
//        byte[] ret;
//        int length = size;
//        //max length 65535/2
//        if(length >= 0 && length <= 127){
//            ret = NumberUtils.toByteArray(length, 0xff);
//        }else if(length >= 128 && length <= 32767){
//            ret = NumberUtils.toByteArray(length, 0xffff);
//            //第一位置成1
//            ret[1] = (byte)((ret[1]) ^ ((byte)0x80));
//        }else{
//            throw new RuntimeException("length must be 1 ~ 32767, but " + size);
//        }
//        return ret;
//    }
//
//    /**
//     *
//     * @param fileIndex
//     * @return
//     * @throws IOException
//     */
//    private MappedByteBuffer selectFile(int fileIndex) throws IOException {
////        Long cursor = maxOffsets.get(startIndex);
////        int fileIndex = calCurrentFileIndex(cursor.longValue());
//        MappedByteBuffer mappedByteBuffer = caches.get(fileIndex);
//        return mappedByteBuffer;
//    }
//
//    /**
//     * 检查当前的mappedByteBuffer 是否有足够的容量
//     * @param byteSize
//     * @return
//     * @throws IOException
//     */
//    private MappedByteBuffer getOrCreateMappedByteBufferWithCapacity(int byteSize) throws IOException {
//
//        int tmpFileIndex = calCurrentFileIndex();
//        MappedByteBuffer buffer = getOrCreateMappedByteBuffer(tmpFileIndex);
//        int leftCapacity = getFileSizeOrDefault() - (buffer.position() + 1);
//        if(leftCapacity <= byteSize){
//            tmpFileIndex += 1;
//            totalMargin = totalMargin + leftCapacity;
//            cursor = cursor + leftCapacity;
//            buffer = getOrCreateMappedByteBuffer(tmpFileIndex);
//        }
//        return buffer;
//    }
//
//    public String getJobName() {
//        return jobName;
//    }
//
//    public void setJobName(String jobName) {
//        this.jobName = jobName;
//    }
//
//    public int getFileSize() {
//        return fileSize;
//    }
//
//    public void setFileSize(int fileSize) {
//        this.fileSize = fileSize;
//    }
//
//    private final int getFileSizeOrDefault(){
//        if(fileSize == -1){
//            fileSize = SIZE;
//        }
//        return fileSize;
//    }
//
//    public List<MappedByteBuffer> getCaches() {
//        return caches;
//    }
//
//    public void setCaches(List<MappedByteBuffer> caches) {
//        this.caches = caches;
//    }
//
//    public static class FilePosition implements Serializable{
//
//        private static final long serialVersionUID = 9128119471875329716L;
//
//        public int fileIndex;
//        public long bufferPosition;
//
//        public FilePosition(int a, long b){
//            fileIndex = a;
//            bufferPosition = b;
//        }
//
//        public int getFileIndex() {
//            return fileIndex;
//        }
//
//        public void setFileIndex(int fileIndex) {
//            this.fileIndex = fileIndex;
//        }
//
//        public long getBufferPosition() {
//            return bufferPosition;
//        }
//
//        public void setBufferPosition(long bufferPosition) {
//            this.bufferPosition = bufferPosition;
//        }
//
//    }
//}