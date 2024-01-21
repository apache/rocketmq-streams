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
package org.apache.rocketmq.streams.dim.model;

import java.io.File;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.streams.common.cache.compress.AbstractMemoryTable;
import org.apache.rocketmq.streams.common.threadpool.ScheduleFactory;
import org.apache.rocketmq.streams.dim.index.DimIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @description 基于本地文件存储的多进程共享的dim
 */
public abstract class AbstractProcShareDim extends AbstractDim {

    static final Logger LOGGER = LoggerFactory.getLogger(AbstractProcShareDim.class);
    static final Object lock = new Object();
    static volatile boolean isFinishCreate = false;
    static AbstractMemoryTable table;
    static DimIndex dimIndex;
    static String cycleStr;
    String filePath;
    /**
     * 多进程文件锁
     */
    private transient File fileLock;
    /**
     * done标, 多进程通过done文件通信
     */
    private transient File doneFile;
    private transient FileChannel lockChannel;

    public AbstractProcShareDim(String filePath) {
        super();
        this.filePath = filePath;
    }

    @Override
    protected boolean initConfigurable() {
        synchronized (lock) {
            ScheduleFactory.getInstance().execute(getNameSpace() + "-" + getName() + "-file_dim_schedule", new Runnable() {
                @Override
                public void run() {
                    loadNameList();
                }
            }, pollingTimeSeconds, pollingTimeSeconds, TimeUnit.MINUTES);
        }
        return true;
    }

    @Override
    protected void loadNameList() {
        try {
            LOGGER.info(getName() + " begin polling data");
            //全表数据
            AbstractMemoryTable dataCacheVar = loadData();
            table = dataCacheVar;
            this.nameListIndex = buildIndex(dataCacheVar);
            this.columnNames = this.dataCache.getCloumnName2Index().keySet();
        } catch (Exception e) {
            LOGGER.error("Load configurables error:" + e.getMessage(), e);
        }
    }

    private void getCycleStr(Date date) {
        String cycleStr = "";
        if (pollingTimeSeconds >= 1 && pollingTimeSeconds < 60) {
            SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
            cycleStr = format.format(date);
        } else if (pollingTimeSeconds >= 60 && pollingTimeSeconds < (60 * 60)) {
            SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmm");
            cycleStr = format.format(date);
        } else if (pollingTimeSeconds >= 60 * 60 && pollingTimeSeconds < (60 * 60 * 24)) {
            SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHH");
            cycleStr = format.format(date);
        } else if (pollingTimeSeconds >= (60 * 60 * 24)) {
            SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
            cycleStr = format.format(date);
        }

    }

}
