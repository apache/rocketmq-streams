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
import java.lang.management.ManagementFactory;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.io.FileUtils;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.utils.DataTypeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MappedByteBufferTable extends FileBasedTable {

    protected static final int DEFAULT_SIZE = 1024 * 1024 * 1024;
    private static final Logger logger = LoggerFactory.getLogger(MappedByteBufferTable.class);
    protected transient List<MappedByteBuffer> caches = new ArrayList<>();
    protected transient List<FileChannel> channels = new ArrayList<>();
    protected transient List<RandomAccessFile> files = new ArrayList<>();
    int fileSize = -1;
    String cycleId;

    private MappedByteBufferTable() {
        super();
    }

    public MappedByteBufferTable(String fileName) {
        this(fileName, -1, DEFAULT_SIZE, null);
    }

    public MappedByteBufferTable(String fileName, String cycleId) {
        this(fileName, -1, DEFAULT_SIZE, null);
        this.cycleId = cycleId;
    }

    public MappedByteBufferTable(String fileName, int columnCount, TableSchema schema) {
        this(fileName, columnCount, DEFAULT_SIZE, schema);
    }

    public MappedByteBufferTable(String fileName, int columnCount, int fileSize, TableSchema schema) {
        super(fileName, columnCount, schema);
        if (fileSize > DEFAULT_SIZE) {
            logger.error("file size exceeds max size " + DEFAULT_SIZE);
            fileSize = DEFAULT_SIZE;
//           throw new IllegalArgumentException("file size exceeds max size " + DEFAULT_SIZE);
        }
        this.fileSize = fileSize;
//       File file = new File("/tmp");
//       List<String> pos = new ArrayList<>();
//       for(File f : file.listFiles()){
//           String tmpFileName = f.getAbsolutePath();
//           if(tmpFileName.startsWith(mappedFilePrefix + fileName + "_")){
//                pos.add(tmpFileName);
//           }
//       }
//       if(pos.size() > 0){
//           String[] array = pos.toArray(new String[0]);
//           Arrays.sort(array);
//           for(String s : array){
//               try {
//                   createMappedByteBuffer(s);
////                   caches.add(buffer);
//               } catch (IOException e) {
//                   e.printStackTrace();
//               }
//           }
//       }
    }

    public void loadFromExistsFile() {
        File file = new File("/tmp");
        List<String> pos = new ArrayList<>();
        for (File f : file.listFiles()) {
            String tmpFileName = f.getAbsolutePath();
            if (tmpFileName.startsWith(mappedFilePrefix + fileName + "_")) {
                pos.add(tmpFileName);
            }
        }
        if (pos.size() > 0) {
            String[] array = pos.toArray(new String[0]);
            Arrays.sort(array);
            for (String s : array) {
                try {
                    createMappedByteBuffer(s);
//                   caches.add(buffer);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    //todo file lock
    private final String createMappedFile(int fileIndex) throws IOException {
        String path = mappedFilePrefix + fileName + "_";
        if (cycleId != null) {
            path = path + cycleId + "_";
        }
        path = path + fileIndex;
        return createMappedFile(path);
    }

    private final String createMappedFile(String path) throws IOException {
        File file = new File(path);
        boolean isExists = file.exists();
        if (!isExists) {
            isExists = file.createNewFile();
        }
        if (isExists) {
            return path;
        } else {
            logger.error(String.format("create mapped file error, file path is %s", path));
            return null;
        }
    }

    private final MappedByteBuffer createMappedByteBuffer(int fileIndex) throws IOException {
        String filePath = createMappedFile(fileIndex);
        return createMappedByteBuffer(filePath);
    }

    private final MappedByteBuffer createMappedByteBuffer(String filePath) throws IOException {
        return createMappedByteBuffer(filePath, getFileSizeOrDefault(-1));
    }

    private final MappedByteBuffer createMappedByteBuffer(String filePath, int limit) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(filePath, "rwd");
        FileChannel fc = raf.getChannel();
        MappedByteBuffer mbb = fc.map(FileChannel.MapMode.READ_WRITE, 0, getFileSizeOrDefault(limit));
        files.add(raf);
        channels.add(fc);
        caches.add(mbb);
        return mbb;
    }

    private final int calCurrentFileIndex(long cursor) {
        int i;
        long totalSize = 0;
        for (i = 0; i < caches.size(); i++) {
            totalSize += caches.get(i).limit();
            if (totalSize > cursor) {
                return i;
            }
        }
        return i;
    }

    private final FilePosition seek(long offset) {

        int fileIndex = calCurrentFileIndex(offset);
        long totalBytes = 0;
        //不减当前文件
        for (int i = 0; i < fileIndex; i++) {
            totalBytes = totalBytes + caches.get(i).limit();
        }
        long bufferPosition = offset - totalBytes;
        return new FilePosition(fileIndex, bufferPosition);
    }

    private byte[] loadByteFromFile(FilePosition position, int len) {
        int fileIndex = position.getFileIndex();
        long offset = position.getBufferPosition();
        MappedByteBuffer buffer = caches.get(fileIndex);
        byte[] dst = new byte[len];
        int index = Long.valueOf(offset).intValue();
        for (int i = 0; i < len; i++) {
            dst[i] = buffer.get(index + i);
        }
        return dst;
    }

    /**
     * @param data
     * @param offset
     * @return 写入的length
     */
    @Override
    long save2File(byte[][] data, long offset) {
        if (data == null || data.length == 0) {
            return 0;
        }
        byte[] lengthByte = data[0];
        int length = rowLenByte2Int(lengthByte);
        int writeSize = length + lengthByte.length;
        try {
            FilePosition position = seek(offset);
            save2FileInner(data, writeSize, position);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return writeSize;
    }

    @Override
    byte[] loadFromFile(long offset, int len) {
        FilePosition position = seek(offset);
        return loadByteFromFile(position, len);
    }

    @Override
    boolean destroy() {
        return false;
    }

    private void save2FileInner(byte[][] data, int byteSize, FilePosition position) throws IOException {
        MappedByteBuffer mappedByteBuffer = nextBuffer(byteSize, position);
        for (byte[] bytes : data) {
            mappedByteBuffer.put(bytes);
        }
    }

    /**
     * 检查当前的mappedByteBuffer 是否有足够的容量, 如果不够则创建下一个buffer
     *
     * @param byteSize
     * @return
     * @throws IOException
     */
    private final MappedByteBuffer nextBuffer(int byteSize, FilePosition position) throws IOException {
        MappedByteBuffer buffer = null;
        if (caches.size() == 0) {
            buffer = createMappedByteBuffer(0);
//            caches.add(buffer);
        } else {
            int index = position.getFileIndex(); //caches.size() - 1;
            buffer = caches.get(index);
            if (buffer.remaining() < byteSize) {
                buffer.flip();
                buffer = createMappedByteBuffer(index + 1);
//                caches.add(buffer);
            }
        }
        return buffer;
    }

    private final int getFileSizeOrDefault(int limit) {
        if (limit > 0) {
            return limit;
        }
        if (fileSize <= 0) {
            fileSize = DEFAULT_SIZE;
        }
        return fileSize;
    }

    public List<MappedByteBuffer> getCaches() {
        return caches;
    }

    public void setCaches(List<MappedByteBuffer> caches) {
        this.caches = caches;
    }

    public int getFileSize() {
        return fileSize;
    }

    public void setFileSize(int fileSize) {
        if (fileSize > DEFAULT_SIZE) {
            fileSize = DEFAULT_SIZE;
        }
        this.fileSize = fileSize;
    }

    public long getByteTotalSize() {
        int size = caches.size();
        if (size == 0) {
            return 0;
        }
        long total = 0;
        for (int i = 0; i < (size - 1); i++) {
            total = total + caches.get(i).limit();
        }
        MappedByteBuffer buffer = caches.get(size - 1);
        total = total + (buffer.limit() - buffer.remaining());
        return total;
    }

    public void loadDataFinished() {
        caches.get(caches.size() - 1).flip();
    }

    public List<FileChannel> getChannels() {
        return channels;
    }

    public void setChannels(List<FileChannel> channels) {
        this.channels = channels;
    }

    public List<RandomAccessFile> getFiles() {
        return files;
    }

    public void setFiles(List<RandomAccessFile> files) {
        this.files = files;
    }

    public interface DateLoader {

        void load(MappedByteBufferTable table);

    }

    public static class FileMeta implements Serializable {

        private static final long serialVersionUID = -248063270830962898L;

        long totalByteSize;
        int totalRowCount;
        int fileSize;
        int columnsCount;
        String[] filePaths;
        Integer[] limits;
        Map<String, Integer> columnName2Index;
        Map<Integer, String> index2ColumnName;
        Map<String, DataType> columns2DataType;

        public static void writeFileMeta(MappedByteBufferTable table, File file) throws IOException {
            List<String> lines = new ArrayList<>();
            lines.add(String.valueOf(table.getFileOffset()));
            lines.add(String.valueOf(table.getFileRowCount()));
            lines.add(String.valueOf(table.getFileSize()));
            lines.add(String.valueOf(table.getColumnsCount()));
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < table.getCaches().size(); i++) {
                String path = table.createMappedFile(i);
                int limit = table.getCaches().get(i).limit();
                sb.append(path).append(",").append(limit);
                if (i != (table.getCaches().size() - 1)) {
                    sb.append(";");
                }
            }
            lines.add(sb.toString());
            StringBuilder sb1 = new StringBuilder();
            Map<Integer, String> index2ColumnName = table.getIndex2ColumnName();
            Map<String, DataType> columnName2DataType = table.getCloumnName2DatatType();
            int rowCount = index2ColumnName.size();
            for (int i = 0; i < rowCount; i++) {
                String columnName = index2ColumnName.get(i);
                DataType type = columnName2DataType.get(columnName);
                sb1.append(columnName).append(",").append(i).append(",").append(type.getDataTypeName());
                if (i != (rowCount - 1)) {
                    sb1.append(";");
                }
            }
            lines.add(sb1.toString());
            FileUtils.writeLines(file, lines);
        }

        public static FileMeta readFileMeta(String filePath) throws IOException {

            FileMeta fileMeta = new FileMeta();
            List<String> lines = FileUtils.readLines(new File(filePath));
            fileMeta.totalByteSize = Long.parseLong(lines.get(0));
            fileMeta.totalRowCount = Integer.parseInt(lines.get(1));
            fileMeta.fileSize = Integer.parseInt(lines.get(2));
            fileMeta.columnsCount = Integer.parseInt(lines.get(3));
            String paths = lines.get(4);
            String columns = lines.get(5);
            String[] pathPairs = paths.split(";");
            fileMeta.filePaths = new String[pathPairs.length];
            fileMeta.limits = new Integer[pathPairs.length];
            fileMeta.columnName2Index = new HashMap<>();
            fileMeta.index2ColumnName = new HashMap<>();
            fileMeta.columns2DataType = new HashMap<>();
            for (int i = 0; i < pathPairs.length; i++) {
                String[] tmp = pathPairs[i].split(",");
                fileMeta.filePaths[i] = tmp[0];
                fileMeta.limits[i] = Integer.parseInt(tmp[1]);
            }
            String[] columnMap = columns.split(";");
            for (int i = 0; i < columnMap.length; i++) {
                String[] tmp = columnMap[i].split(",");
                fileMeta.columnName2Index.put(tmp[0], Integer.valueOf(tmp[1]));
                fileMeta.index2ColumnName.put(Integer.valueOf(tmp[1]), tmp[0]);
                fileMeta.columns2DataType.put(tmp[0], DataTypeUtil.getDataType(tmp[2]));
            }
            return fileMeta;
        }

        @Override
        public String toString() {
            return "FileMeta{" +
                "totalByteSize=" + totalByteSize +
                ", totalRowCount=" + totalRowCount +
                ", fileSize=" + fileSize +
                ", columnsCount=" + columnsCount +
                ", filePaths=" + Arrays.toString(filePaths) +
                ", limits=" + Arrays.toString(limits) +
                ", columnName2Index=" + columnName2Index +
                ", index2ColumnName=" + index2ColumnName +
                ", columns2DataType=" + columns2DataType +
                '}';
        }
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

    public static class Creator {

        Properties properties = new Properties();
        String filePath;
        String cycleId;
        private File fileLock;
        private File doneFile;
        private FileChannel lockChannel;

        public static Creator newCreator(String filePath, Date date, int pollingTimeMinute) throws IOException {
            Creator creator = new Creator();
            creator.filePath = filePath;
            creator.cycleId = creator.getCycleStr(date, pollingTimeMinute);
            creator.init();
            return creator;
        }

        public Creator setFilePath(String filePath) {
            this.filePath = filePath;
            return this;
        }

        public Creator configure(Properties properties) {
            this.properties.putAll(properties);
            return this;
        }

        private void init() throws IOException {
            String lockPath = FileBasedTable.createLockFilePath(filePath, cycleId);
            fileLock = new File(lockPath);
            if (!fileLock.exists()) {
                fileLock.createNewFile();
            }
            lockChannel = new RandomAccessFile(fileLock, "rw").getChannel();
            String donePath = FileBasedTable.createDoneFilePath(filePath, cycleId);
            doneFile = new File(donePath);
        }

        public MappedByteBufferTable create(DateLoader loader) throws IOException {
//            init(cycleId);
            MappedByteBufferTable table = new MappedByteBufferTable(filePath, cycleId);
            String fileSize = properties.getProperty("fileSize");
            if (fileSize != null) {
                table.setFileSize(Integer.parseInt(fileSize));
            }
            FileLock lock = null;
            boolean initByCreate = false;
            try {
                while (!doneFile.exists()) {
                    lock = lockChannel.tryLock();
                    while (lock == null) {
                        if (doneFile.exists()) {
                            break;
                        }
                        Thread.sleep(1000);
                        System.out.println(ManagementFactory.getRuntimeMXBean().getName() + " wait lock......");
                        lock = lockChannel.tryLock();
                    }
                    System.out.println(ManagementFactory.getRuntimeMXBean().getName() + " get lock");
                    if (doneFile.exists()) {
                        System.out.println(ManagementFactory.getRuntimeMXBean().getName() + " get done");
                        break;
                    } else {
                        System.out.println(ManagementFactory.getRuntimeMXBean().getName() + " create");
                        createInner(loader, table);
                        initByCreate = true;
//                        table.loadDataFinished();
                        FileMeta.writeFileMeta(table, doneFile);
                        doneFile.createNewFile();
                        System.out.println(ManagementFactory.getRuntimeMXBean().getName() + " write done");
                    }
                }
                if (!initByCreate) {
                    System.out.println(ManagementFactory.getRuntimeMXBean().getName() + " file already exists, will load.");
                    loadFromFile(table);
                }
                release();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException exception) {
                exception.printStackTrace();
            } finally {
                if (lock != null && lock.acquiredBy().isOpen()) {
                    try {
                        lock.release();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            return table;
        }

        private void createInner(DateLoader loader, MappedByteBufferTable table) {
            loader.load(table);
            table.loadDataFinished();
        }

        private void loadFromFile(MappedByteBufferTable table) throws IOException {
            String filePath = this.doneFile.getAbsolutePath();
            FileMeta meta = FileMeta.readFileMeta(filePath);
            table.setFileOffset(meta.totalByteSize);
            table.setColumnsCount(meta.columnsCount);
            table.setFileRowCount(meta.totalRowCount);
            table.setColumnName2Index(meta.columnName2Index);
            table.setIndex2ColumnName(meta.index2ColumnName);
            table.setColumnName2DatatType(meta.columns2DataType);
            for (int i = 0; i < meta.filePaths.length; i++) {
                String path = meta.filePaths[i];
                int limit = meta.limits[i];
                table.createMappedByteBuffer(path, limit);
            }
        }

        private void release() {

        }

        private String getCycleStr(Date date, int pollingTimeSecond) {
            String cycleStr = "";
            if (pollingTimeSecond >= 1 && pollingTimeSecond < 60) {
                SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
                cycleStr = format.format(date);
            } else if (pollingTimeSecond >= 60 && pollingTimeSecond < (60 * 60)) {
                SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmm");
                cycleStr = format.format(date);
            } else if (pollingTimeSecond >= 60 * 60 && pollingTimeSecond < (60 * 60 * 24)) {
                SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHH");
                cycleStr = format.format(date);
            } else if (pollingTimeSecond >= (60 * 60 * 24)) {
                SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
                cycleStr = format.format(date);
            }
            return cycleStr;
        }

    }
}