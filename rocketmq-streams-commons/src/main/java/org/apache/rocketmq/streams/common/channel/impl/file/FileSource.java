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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.streams.common.batchsystem.BatchFinishMessage;
import org.apache.rocketmq.streams.common.channel.source.AbstractBatchSource;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.utils.DateUtil;

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

    public FileSource(String filePath) {
        this();
        this.filePath = filePath;
    }

    @Override
    protected boolean initConfigurable() {
        super.initConfigurable();
        File file = getFile(filePath);
        if (file.exists() && file.isDirectory()) {
            executorService = new ThreadPoolExecutor(maxThread, maxThread, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(1000));
        }
        return true;
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
            throw new RuntimeException("filePath not exist.the filePath is "+filePath);
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
                while (fileIterator.hasNext()) {
                    String line = fileIterator.next();
                    doReceiveMessage(line, false, fileIterator.file.getName(), offset + "");
                    offset++;
                    count.incrementAndGet();

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

        } catch (IOException e) {
            String realFilePath = filePath;
            throw new RuntimeException("close error " + realFilePath, e);
        }
    }

    public FileSource() {
        setType(ISource.TYPE);
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

}


