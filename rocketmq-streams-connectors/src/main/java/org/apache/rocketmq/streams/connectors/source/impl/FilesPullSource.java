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
package org.apache.rocketmq.streams.connectors.source.impl;

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.streams.common.channel.impl.file.FileSource;
import org.apache.rocketmq.streams.common.channel.impl.file.FileSplit;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.context.MessageOffset;
import org.apache.rocketmq.streams.connectors.model.PullMessage;
import org.apache.rocketmq.streams.connectors.reader.AbstractSplitReader;
import org.apache.rocketmq.streams.connectors.reader.ISplitReader;
import org.apache.rocketmq.streams.connectors.source.AbstractPullSource;

public class FilesPullSource extends AbstractPullSource {

    protected String filePath;//一个文件一个分片
    protected String namePattern;//可以配置正则过滤需要的文件
    protected boolean isCSV = false;

    protected transient FileSource fileSource;

    @Override protected boolean initConfigurable() {
        FileSource fileSource = new FileSource();
        fileSource.setFilePath(filePath);
        fileSource.setNamePattern(namePattern);
        fileSource.setCSV(isCSV);
        fileSource.init();
        this.fileSource = fileSource;
        return super.initConfigurable();
    }

    @Override protected ISplitReader createSplitReader(ISplit<?, ?> split) {
        return new AbstractSplitReader(split) {
            private FileSource.FileIterator iterator;
            private AtomicInteger offsetGenerator = new AtomicInteger(0);
            private transient List<JSONObject> csvFileMsgs;
            private transient int nextCurrentIndex = 0;
            private transient boolean hasNext = true;

            @Override public String getCursor() {
                if (isCSV) {
                    return this.nextCurrentIndex + "";
                }
                return offsetGenerator.get() + "";
            }

            @Override public void open() {
                FileSplit fileSplit = (FileSplit) split;
                this.cursor = 0 + "";
                try {
                    iterator = new FileSource.FileIterator(fileSplit.getFile());
                } catch (Exception e) {
                    throw new RuntimeException("创建文件迭代器错误：" + fileSplit.getFile().getAbsolutePath(), e);
                }

            }

            @Override public boolean next() {
                if (isCSV) {
                    if (csvFileMsgs == null) {
                        return true;
                    }
                    if (nextCurrentIndex <= csvFileMsgs.size() - 1) {
                        return true;
                    }
                    return false;
                }
                return hasNext;
            }

            @Override public Iterator<PullMessage<?>> getMessage() {
                List<PullMessage<?>> pullMessages = new ArrayList<>();
                if (isCSV) {
                    if (csvFileMsgs == null) {
                        csvFileMsgs = fileSource.parseCSV(iterator.getFile());
                        ;
                    }
                    int startIndex = nextCurrentIndex;
                    for (int i = startIndex; i < csvFileMsgs.size(); i++) {
                        PullMessage pullMessage = new PullMessage();
                        JSONObject jsonObject = csvFileMsgs.get(i);
                        pullMessage.setMessage(jsonObject);
                        MessageOffset messageOffset = new MessageOffset(i + 1);
                        pullMessage.setMessageOffset(messageOffset);
                        pullMessages.add(pullMessage);
                        nextCurrentIndex = i + 1;
                    }
                    return pullMessages.iterator();
                } else {
                    while (iterator.hasNext()) {
                        String line = iterator.next();
                        if (line == null) {
                            hasNext = false;
                            break;
                        }
                        PullMessage<JSONObject> pullMessage = new PullMessage<>();
                        JSONObject jsonObject = fileSource.create(line);
                        pullMessage.setMessage(jsonObject);
                        MessageOffset messageOffset = new MessageOffset(offsetGenerator.incrementAndGet());
                        pullMessage.setMessageOffset(messageOffset);
                        pullMessages.add(pullMessage);
                    }

                    return pullMessages.iterator();

                }
            }

            @Override public void seek(String cursor) {
                if (isCSV) {
                    nextCurrentIndex = Integer.parseInt(cursor);
                } else {
                    offsetGenerator = new AtomicInteger(Integer.parseInt(cursor));
                }
            }

            @Override public long getDelay() {
                return 0;
            }

            @Override public long getFetchedDelay() {
                return 0;
            }
        };
    }

    @Override public List<ISplit<?, ?>> fetchAllSplits() {
        Iterator<FileSource.FileIterator> it = fileSource.createIteratorList().iterator();
        List<ISplit<?, ?>> splits = new ArrayList<>();
        while (it.hasNext()) {
            FileSource.FileIterator fileIterator = it.next();
            splits.add(new FileSplit(fileIterator.getFile()));
        }
        return splits;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public String getNamePattern() {
        return namePattern;
    }

    public void setNamePattern(String namePattern) {
        this.namePattern = namePattern;
    }

    public boolean isCSV() {
        return isCSV;
    }

    public void setCSV(boolean CSV) {
        isCSV = CSV;
    }
}
