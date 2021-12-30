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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.cache.compress.AbstractMemoryTable;
import org.apache.rocketmq.streams.dim.index.DimIndex;

/**
 * @description 基于本地文件存储的多进程共享的dim
 */
public abstract class AbstractProcShareDim extends AbstractDim {

    static final Log logger = LogFactory.getLog(AbstractProcShareDim.class);

    /**
     * 多进程文件锁
     */
    private transient File fileLock;
    /**
     * done标, 多进程通过done文件通信
     */
    private transient File doneFile;
    private transient FileChannel lockChannel;
    String filePath;

    static final Object lock = new Object();
    static volatile boolean isFinishCreate = false;
    static AbstractMemoryTable table;
    static ScheduledExecutorService executorService;
    static DimIndex dimIndex;
    static String cycleStr;

    public AbstractProcShareDim(String filePath) {
        super();
        this.filePath = filePath;
    }

    @Override
    protected boolean initConfigurable() {
        if (executorService == null) {
            synchronized (lock) {
                if (executorService == null) {
                    executorService = new ScheduledThreadPoolExecutor(1);
                    executorService.scheduleAtFixedRate(new Runnable() {
                        @Override
                        public void run() {
                            loadNameList();
                        }
                    }, pollingTimeMinute, pollingTimeMinute, TimeUnit.MINUTES);
                }
            }
        }
//        boolean old = Boolean.parseBoolean(ComponentCreator.getProperties().getProperty(ConfigureFileKey.DIPPER_RUNNING_STATUS));
//        ComponentCreator.getProperties().put(ConfigureFileKey.DIPPER_RUNNING_STATUS, false);
//        boolean success = super.initConfigurable();
//        ComponentCreator.getProperties().put(ConfigureFileKey.DIPPER_RUNNING_STATUS, old);
        return true;
    }

    protected void loadNameList() {
        try {
            logger.info(getConfigureName() + " begin polling data");
            //全表数据
            AbstractMemoryTable dataCacheVar = loadData();
            table = dataCacheVar;
            this.nameListIndex = buildIndex(dataCacheVar);
            this.columnNames = this.dataCache.getCloumnName2Index().keySet();
        } catch (Exception e) {
            logger.error("Load configurables error:" + e.getMessage(), e);
        }
    }

    private void getCycleStr(Date date) {
        if (pollingTimeMinute >= 1 && pollingTimeMinute < 60) {
            SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmm");
            cycleStr = format.format(date);
        } else if (pollingTimeMinute >= 60 && pollingTimeMinute < (60 * 24)) {
            SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHH");
            cycleStr = format.format(date);
        } else if (pollingTimeMinute >= (60 * 24)) {
            SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
            cycleStr = format.format(date);
        }
    }

}
