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
package org.apache.rocketmq.streams.common.channel.impl.file;

import com.alibaba.fastjson.JSONObject;
import com.univocity.parsers.common.processor.RowListProcessor;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.streams.common.batchsystem.BatchFinishMessage;
import org.apache.rocketmq.streams.common.channel.source.AbstractBatchSource;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;
import org.apache.rocketmq.streams.common.threadpool.ThreadPoolFactory;
import org.apache.rocketmq.streams.common.utils.ContantsUtil;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;

/**
 * 文件的输入输出，source是把指定的文件数据加载到内存，for循环输出到后续节点 sink，把内容写入文件，可以配置写入模式，是追加还是覆盖
 */
public class FileSource extends AbstractBatchSource {

    /**
     * 文件全路径。如果目录是环境变量，可以写成evnDir/文件名
     */
    @ENVDependence
    private String filePath;

    protected transient BufferedReader reader;
    protected transient ExecutorService executorService;
    protected boolean isCSV = false;
    protected transient String[] fieldNames;
    protected transient Object locker = new Object();

    public FileSource(String filePath) {
        this();
        this.filePath = filePath;
    }

    @Override
    protected boolean initConfigurable() {
        super.initConfigurable();
        File file = getFile(filePath);
        if (file.exists() && file.isDirectory()) {
            if (executorService == null) {
                executorService = ThreadPoolFactory.createFixedThreadPool(maxThread, FileSource.class.getName() + "-" + getConfigureName());
            }
        }
        if (file.exists() && !file.isDirectory()) {
            if (executorService == null) {
                executorService = ThreadPoolFactory.createFixedThreadPool(1, FileSource.class.getName() + "-" + getConfigureName());
            }
        }
        return super.initConfigurable();
    }

    private File getFile(String filePath) {
        File file = new File(filePath);
        if (!file.exists()) {
            ClassLoader loader = getClass().getClassLoader();
            URL url = loader.getResource(filePath);

            if (url != null) {
                String path = url.getFile();
                file = new File(path);
                this.filePath = path;
            }
        }
        return file;

    }

    @Override
    protected boolean startSource() {
        LinkedBlockingQueue<FileIterator> queue = createIteratorList();
        AtomicInteger count = new AtomicInteger(0);
        long startTime = System.currentTimeMillis();
        CountDownLatch countDownLatch = new CountDownLatch(queue.size());
        try {
            FileIterator fileIterator = queue.poll();

            while (fileIterator != null) {
                ReadTask readTask = new ReadTask(fileIterator, count, countDownLatch);
                if (executorService != null) {
                    executorService.execute(readTask);
                } else {
                    readTask.run();
                }
                fileIterator = queue.poll();
            }
            countDownLatch.await();
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("process data cost :" + (System.currentTimeMillis() - startTime) + ", the count is " + count.get() + " now " + DateUtil.getCurrentTimeString());

        return true;
    }

    /**
     * 如果是目录，每个文件一个iterator，如果是文件只生成一个iterator
     */
    protected LinkedBlockingQueue<FileIterator> createIteratorList() {
        LinkedBlockingQueue<FileIterator> iterators = new LinkedBlockingQueue<>(1000);
        File file = getFile(filePath);
        if (!file.exists()) {
            return null;
        }
        try {
            if (!file.isDirectory()) {
                iterators.put(new FileIterator(file));
                return iterators;
            }
            File[] files = file.listFiles();
            for (File subFile : files) {
                iterators.add(new FileIterator(subFile));
            }
            return iterators;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * 一个文件生成一个Iterator，每次加载一行数据
     */
    public static class FileIterator implements Iterator<String> {
        protected File file;
        private String line;
        protected int index = 0;
        protected BufferedReader reader = null;

        public FileIterator(File file) throws FileNotFoundException {
            this.file = file;
            reader = new BufferedReader(new FileReader(file));
        }

        @Override
        public boolean hasNext() {
            try {
                line = reader.readLine();
                index++;
                if (line != null) {
                    return true;
                }
            } catch (IOException e) {
                throw new RuntimeException("read error ", e);
            }

            return false;
        }

        public int getIndex() {
            return index;
        }

        public void close() {
            try {
                reader.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public String next() {
            return line;
        }
    }

    /**
     * 如果是个目录，1个文件一个线程
     */
    protected class ReadTask implements Runnable {
        protected FileIterator fileIterator;
        protected AtomicInteger count;
        private CountDownLatch countDownLatch;

        public ReadTask(FileIterator fileIterator, AtomicInteger count, CountDownLatch countDownLatch) {
            this.fileIterator = fileIterator;
            this.count = count;
            this.countDownLatch = countDownLatch;
        }

        @Override
        public void run() {
            if (fileIterator != null) {
                int offset = 1;
                if (isCSV) {
                    List<JSONObject> messages = parseCSV(fileIterator.file);
                    for (JSONObject msg : messages) {
                        doReceiveMessage(msg, false, fileIterator.file.getName(), offset + "");
                        offset++;
                        count.incrementAndGet();
                    }
                } else {

                    while (fileIterator.hasNext()) {
                        String line = fileIterator.next();
                        doReceiveMessage(line, false, fileIterator.file.getName(), offset + "");
                        offset++;
                        count.incrementAndGet();
                    }

                }
                sendCheckpoint(fileIterator.file.getName());
                executeMessage((Message) BatchFinishMessage.create());
                fileIterator.close();
                countDownLatch.countDown();
            }

        }
    }

    @Override
    public void destroy() {

        try {
            if (reader != null) {
                reader.close();
            }
            if (this.executorService != null) {
                this.executorService.shutdown();
                this.executorService = null;
            }
        } catch (IOException e) {
            String realFilePath = filePath;
            throw new RuntimeException("close error " + realFilePath, e);
        }
    }

    public FileSource() {
        setType(ISource.TYPE);
    }

    @Override public List<ISplit<?, ?>> getAllSplits() {
        File file = getFile(filePath);
        ISplit<?,?> split= new FileSplit(file);
        List<ISplit<?,?>> splits=new ArrayList<>();
        splits.add(split);
        return splits;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public static void main(String[] args) {
        FileSource fileSource = new FileSource("/Users/yuanxiaodong/Downloads/sample.csv");
        fileSource.setCSV(true);
        fileSource.init();
        fileSource.start((message, context) -> {
            if (message.getHeader().isSystemMessage()) {
                return null;
            }
            System.out.println(message.getMessageBody());
            return null;
        });
    }

    public List<JSONObject> parseCSV(File file) {
        //创建一个配置选项，用来提供多种配置选项
        CsvParserSettings parserSettings = new CsvParserSettings();
        //打开解析器的自动检测功能，让它自动检测输入中包含的分隔符
        parserSettings.setLineSeparatorDetectionEnabled(true);

        //创建RowListProcessor对象，用来把每个解析的行存储在列表中
        RowListProcessor rowListProcessor = new RowListProcessor();
        parserSettings.setProcessor(rowListProcessor);  //配置解析器
        //待解析的CSV文件包含标题头，把第一个解析行看作文件中每个列的标题
        parserSettings.setHeaderExtractionEnabled(true);
        parserSettings.setLineSeparatorDetectionEnabled(true);

        //创建CsvParser对象，用于解析文件
        CsvParser parser = new CsvParser(parserSettings);
        parser.parse(file);

        //如果解析中包含标题，用于获取标题
        String[] headers = rowListProcessor.getHeaders();
        //获取行值，并遍历打印
        List<String[]> rows = rowListProcessor.getRows();
        List<JSONObject> messages = new ArrayList<>();
        for (String[] row : rows) {
            JSONObject message = new JSONObject();
            for (int i = 0; i < headers.length; i++) {
                message.put(headers[i], row[i]);
            }
            messages.add(message);
        }
        /*for(int i = 0; i < rows.size(); i++){
            System.out.println(Arrays.asList(rows.get(i)));
        }*/
        return messages;
    }

    public boolean isCSV() {
        return isCSV;
    }

    public void setCSV(boolean CSV) {
        isCSV = CSV;
    }
}


