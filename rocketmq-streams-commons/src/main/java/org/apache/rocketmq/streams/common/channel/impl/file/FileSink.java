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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.streams.common.channel.sink.AbstractSupportShuffleSink;
import org.apache.rocketmq.streams.common.channel.sink.ISink;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.PrintUtil;

/**
 * 文件的输入输出，source是把指定的文件数据加载到内存，for循环输出到后续节点 sink，把内容写入文件，可以配置写入模式，是追加还是覆盖
 */
public class FileSink extends AbstractSupportShuffleSink {

    /**
     * 文件全路径。如果目录是环境变量，可以写成evnDir/文件名
     */
    @ENVDependence
    private String filePath;

    /**
     * 文件写入时，是否追加
     */
    private boolean needAppend = true;

    protected transient BufferedWriter writer;
    /**
     * FileChannel 中 writer 完成 初始化 标识
     */
    private volatile transient boolean writerInitFlag = false;

    public FileSink(String filePath) {
        this(filePath, false);

    }

    public FileSink(String filePath, boolean needAppend) {
        this();
        this.filePath = filePath;
        this.needAppend = needAppend;

    }

    @Override
    public String getShuffleTopicFieldName() {
        return "filePath";
    }

    @Override
    protected void createTopicIfNotExist(int splitNum) {

    }

    @Override
    public List<ISplit> getSplitList() {
        File file = new File(filePath);
        List<ISplit> splits = new ArrayList<>();
        splits.add(new FileSplit(file));
        return splits;
    }

    @Override
    public int getSplitNum() {
        return 1;
    }

    @Override
    protected boolean batchInsert(List<IMessage> messages) {
        // 初始化 write 防止 文件不存在导致异常
        initWrite();
        if (messages != null) {
            try {
                for (IMessage message : messages) {
                    writer.write(message.getMessageValue().toString() + PrintUtil.LINE);
                }
                writer.flush();
            } catch (IOException e) {
                throw new RuntimeException("write line error " + filePath, e);
            }
        }
        return true;
    }

    @Override
    public void destroy() {

        try {
            if (writer != null) {
                writer.flush();
                writer.close();
            }

        } catch (IOException e) {
            throw new RuntimeException("close error " + filePath, e);
        }
    }

    public FileSink() {
        setType(ISink.TYPE);
    }

    private static final String PREFIX = "dipper.upgrade.channel.file.envkey";

    /**
     * 初始化 witer 防止文件不存在异常
     */
    private void initWrite() {
        if (!writerInitFlag) {
            synchronized (this) {
                if (!writerInitFlag) {
                    try {
                        writer = new BufferedWriter(new FileWriter(filePath, needAppend));
                        writerInitFlag = true;
                    } catch (Exception e) {
                        throw new RuntimeException("create write error " + filePath, e);
                    }
                }
            }
        }
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public boolean isNeedAppend() {
        return needAppend;
    }

    public void setNeedAppend(boolean needAppend) {
        this.needAppend = needAppend;
    }

}


