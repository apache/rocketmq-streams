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
package org.apache.rocketmq.streams.db.driver.batchloader;

import com.alibaba.fastjson.JSONObject;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.streams.common.cache.compress.impl.IntValueKV;
import org.apache.rocketmq.streams.common.threadpool.ThreadPoolFactory;
import org.apache.rocketmq.streams.common.utils.SQLUtil;
import org.apache.rocketmq.streams.db.driver.DriverBuilder;
import org.apache.rocketmq.streams.db.driver.JDBCDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 多线程批量加载数据，每加载一批数据后，通过IRowOperator回调接口处理数据 需要有递增的字段，这个字段有索引，不重复，如id字段
 */
public class BatchRowLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchRowLoader.class);
    protected static final int MAX_LINE = 5000;//每个批次最大行数，根据这个值划分并行任务
    protected static ExecutorService executorService = null;
    protected String idFieldName;//配置字段名称，这个字段的值是数字的，且是递增的
    protected String sql;//查询的sql语句，类似select * from table where idFieldName>#{idFieldName=0} order by idFieldName.不要加limit，系统会自动添加
    protected int batchSize = 1000;//每批从数据库加载的数据量
    protected IRowOperator dataRowProcessor;//加载的数据由这个回调接口处理
    private final JDBCDriver jdbcDriver;

    public BatchRowLoader(String idFieldName, String sql, IRowOperator dataRowProcessor) {
        this.idFieldName = idFieldName;
        this.sql = sql;
        this.dataRowProcessor = dataRowProcessor;
        this.jdbcDriver = DriverBuilder.createDriver();
        executorService = ThreadPoolFactory.createFixedThreadPool(20, BatchRowLoader.class.getName() + "-" + idFieldName);
    }

    public BatchRowLoader(String idFieldName, String sql, IRowOperator dataRowProcessor, String driver, String url, String userName, String password) {
        this.idFieldName = idFieldName;
        this.sql = sql;
        this.dataRowProcessor = dataRowProcessor;
        this.jdbcDriver = DriverBuilder.createDriver(driver, url, userName, password);
        executorService = ThreadPoolFactory.createFixedThreadPool(20, BatchRowLoader.class.getName() + "-" + idFieldName);
    }

    public void startLoadData() {
        try {
            String statisticalSQL = sql;
            int startIndex = sql.toLowerCase().indexOf("from");
            statisticalSQL = "select count(1) as c, min(" + idFieldName + ") as min, max(" + idFieldName + ") as max " + sql.substring(startIndex);
            List<Map<String, Object>> rows = jdbcDriver.queryForList(statisticalSQL);
            Map<String, Object> row = rows.get(0);
            int count = Integer.parseInt(row.get("c").toString());
            if (count == 0) {
                LOGGER.debug("there is no data during execute sql: " + statisticalSQL);
                return;
            }

            IntValueKV intValueKV = new IntValueKV(count);
            //int maxBatch=count/maxSyncCount;//每1w条数据，一个并发。如果数据量比较大，为了提高性能，并行执行

            long min = Long.parseLong(row.get("min").toString());
            long max = Long.parseLong(row.get("max").toString());
            int maxSyncCount = count / MAX_LINE + 1;
            long step = (max - min + 1) / maxSyncCount;
            CountDownLatch countDownLatch = new CountDownLatch(maxSyncCount + 1);
            AtomicInteger finishedCount = new AtomicInteger(0);
            String taskSQL = null;
            if (sql.contains(" where ")) {
                taskSQL = sql + " and " + idFieldName + ">#{startIndex} and " + idFieldName + "<=#{endIndex} order by " + idFieldName + " limit " + batchSize;
            } else {
                taskSQL = sql + " where " + idFieldName + ">#{startIndex} and " + idFieldName + "<=#{endIndex} order by " + idFieldName + " limit " + batchSize;
            }

            int i = 0;
            for (; i < maxSyncCount; i++) {
                FetchDataTask fetchDataTask = new FetchDataTask(taskSQL, (min - 1) + step * i, (min - 1) + step * (i + 1), countDownLatch, finishedCount, jdbcDriver, count);
                executorService.execute(fetchDataTask);
            }
            FetchDataTask fetchDataTask = new FetchDataTask(taskSQL, (min - 1) + step * i, (min - 1) + step * (i + 1), countDownLatch, finishedCount, jdbcDriver, count);
            executorService.execute(fetchDataTask);

            countDownLatch.await();

            LOGGER.info(getClass().getSimpleName() + " load data finish, load data line  size is " + count);
        } catch (Exception e) {
            LOGGER.error("failed loading data batch!", e);
        } finally {
            if (jdbcDriver != null) {
                jdbcDriver.destroy();
            }
            if (executorService != null) {
                executorService.shutdown();
                executorService = null;
            }
        }
    }

    protected class FetchDataTask implements Runnable {
        long startIndex;
        long endIndex;
        String sql;
        CountDownLatch countDownLatch;
        JDBCDriver resource;
        AtomicInteger finishedCount;//完成了多少条
        int totalSize;//一共有多少条数据

        public FetchDataTask(String sql, long startIndex, long endIndex, CountDownLatch countDownLatch, AtomicInteger finishedCount, JDBCDriver resource, int totalSize) {
            this.startIndex = startIndex;
            this.endIndex = endIndex;
            this.countDownLatch = countDownLatch;
            this.sql = sql;
            this.finishedCount = finishedCount;
            this.resource = resource;
            this.totalSize = totalSize;
        }

        @Override public void run() {
            long currentIndex = startIndex;
            JSONObject msg = new JSONObject();
            msg.put("endIndex", endIndex);
            while (true) {
                try {
                    msg.put("startIndex", currentIndex);
                    String sql = SQLUtil.parseIbatisSQL(msg, this.sql);
                    List<Map<String, Object>> rows = resource.queryForList(sql);
                    if (rows == null || rows.size() == 0) {
                        break;
                    }
                    currentIndex = Long.parseLong(rows.get(rows.size() - 1).get(idFieldName).toString());

                    int size = rows.size();
                    int count = finishedCount.addAndGet(size);
                    double progress = (double) count / (double) totalSize;
                    progress = progress * 100;
                    System.out.println(" finished count is " + count + " the total count is " + totalSize + ", the progress is " + String.format("%.2f", progress) + "%");
                    if (size < batchSize) {
                        if (size > 0) {
                            doProcess(rows);
                        }
                        break;
                    }
                    doProcess(rows);
                } catch (Exception e) {
                    throw new RuntimeException("put data error ", e);
                }
            }

            countDownLatch.countDown();
        }
    }

    private void doProcess(List<Map<String, Object>> rows) {
        for (Map<String, Object> row : rows) {
            dataRowProcessor.doProcess(row);
        }
    }
}

