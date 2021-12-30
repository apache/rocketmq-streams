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
package org.apache.rocketmq.streams.connectors.reader;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.component.AbstractComponent;
import org.apache.rocketmq.streams.common.context.MessageOffset;
import org.apache.rocketmq.streams.common.utils.ThreadUtil;
import org.apache.rocketmq.streams.connectors.IBoundedSource;
import org.apache.rocketmq.streams.connectors.IBoundedSourceReader;
import org.apache.rocketmq.streams.connectors.model.PullMessage;
import org.apache.rocketmq.streams.connectors.model.ReaderStatus;
import org.apache.rocketmq.streams.connectors.source.CycleDynamicMultipleDBScanSource;
import org.apache.rocketmq.streams.db.driver.DriverBuilder;
import org.apache.rocketmq.streams.db.driver.JDBCDriver;
import org.apache.rocketmq.streams.db.driver.orm.ORMUtil;

/**
 * @description
 */
public class DBScanReader implements ISplitReader, IBoundedSourceReader, Serializable {

    private static final long serialVersionUID = 8172403250050893288L;
    private static final Log logger = LogFactory.getLog(DBScanReader.class);
    static final String sqlTemplate = "select * from %s where id >= %d and id < %d";

    //是否完成了source的call back调用
    transient volatile boolean isFinishedCall = false;
    ISource iSource;
    String url;
    String userName;
    String password;
    String tableName;
    int batchSize;
    long offset;
    long offsetStart;
    long offsetEnd;
    long maxOffset;
    long minOffset;
    ISplit iSplit;
    transient List<PullMessage> pullMessages;
    volatile boolean interrupt = false;
    volatile boolean isClosed = false;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public ISplit getISplit() {
        return iSplit;
    }

    public void setISplit(ISplit iSplit) {
        this.iSplit = iSplit;
    }

    public DBScanReader() {

    }

    transient ThreadLocal<JDBCDriver> threadLocal = new ThreadLocal<JDBCDriver>() {

        @Override
        public JDBCDriver initialValue() {
            logger.info(String.format("%s initial jdbcDriver. ", Thread.currentThread().getName()));
            return DriverBuilder.createDriver(AbstractComponent.DEFAULT_JDBC_DRIVER, url, userName, password);
        }

    };

    @Override
    public void open(ISplit split) {
        this.iSplit = split;
        JDBCDriver jdbcDriver = threadLocal.get();
        Map<String, Object> range = jdbcDriver.queryOneRow("select min(id) as min_id, max(id) as max_id from " + tableName);
        minOffset = Long.parseLong(String.valueOf(range.get("min_id")));
        maxOffset = Long.parseLong(String.valueOf(range.get("max_id")));
        offsetStart = minOffset;
        offset = minOffset;
        logger.info(String.format("table %s min id [ %d ],  max id [ %d ]", tableName, minOffset, maxOffset));
        pullMessages = new ArrayList<>();
    }

    @Override
    public boolean next() {
        if (interrupt) {
            return false;
        }
        if (isFinished()) {
            finish();
            ThreadUtil.sleep(10 * 1000);
            return false;
        }
        JDBCDriver jdbcDriver = threadLocal.get();
        offsetEnd = offsetStart + batchSize;
        String batchQuery = String.format(sqlTemplate, tableName, offsetStart, offsetEnd);
        logger.debug(String.format("execute sql : %s", batchQuery));
        List<Map<String, Object>> resultData = jdbcDriver.queryForList(batchQuery);
        offsetStart = offsetEnd;
        pullMessages.clear();
        for (Map<String, Object> r : resultData) {
            PullMessage msg = new PullMessage();
            JSONObject data = JSONObject.parseObject(JSON.toJSONString(r));
            msg.setMessage(data);
            offset = offset > Long.parseLong(data.getString("id")) ? offset : Long.parseLong(data.getString("id"));
            msg.setMessageOffset(new MessageOffset(String.valueOf(offset), true));
            pullMessages.add(msg);
        }
        return offsetStart - batchSize <= maxOffset;
    }

    @Override
    public List<PullMessage> getMessage() {
//        logger.info(String.format("output messages %d", pullMessages.size()));
        return pullMessages;
    }

    @Override
    public SplitCloseFuture close() {
//        interrupt = true;
        isClosed = true;
        threadLocal.remove();
        pullMessages = null;
        return new SplitCloseFuture(this, iSplit);
    }

    @Override
    public void seek(String cursor) {
        if (cursor == null || cursor.trim().equals("")) {
            cursor = "0";
        }
        offset = Long.parseLong(cursor);
        if (offset < minOffset) {
            offset = minOffset;
        }
        offsetStart = offset;
        logger.info(String.format("split %s seek %d.", iSplit.getQueueId(), offset));
    }

    @Override
    public String getProgress() {
        return String.valueOf(offset);
    }

    @Override
    public long getDelay() {
        return maxOffset - offset;
    }

    @Override
    public long getFetchedDelay() {
        return 0;
    }

    @Override
    public boolean isClose() {
        return isClosed;
    }

    @Override
    public ISplit getSplit() {
        return iSplit;
    }

    @Override
    public boolean isInterrupt() {
        return interrupt;
    }

    @Override
    public boolean interrupt() {
        interrupt = true;
        return true;
    }

    @Override
    public boolean isFinished() {
        return offsetStart > maxOffset;
    }

    @Override
    public void finish() {
        if (isFinishedCall) {
            return;
        }
        pullMessages = null;
        updateReaderStatus();
        IBoundedSource tmp = (IBoundedSource) iSource;
        tmp.boundedFinishedCallBack(this.iSplit);
        isFinishedCall = true;
    }

    public ISource getISource() {
        return iSource;
    }

    public void setISource(ISource iSource) {
        this.iSource = iSource;
    }

    private final void updateReaderStatus() {
        String sourceName = CycleDynamicMultipleDBScanSource.createKey(this.getISource());
        int finish = Integer.valueOf(1);
        int total = ((CycleDynamicMultipleDBScanSource) iSource).getTotalReader();
        ReaderStatus readerStatus = ReaderStatus.create(sourceName, iSplit.getQueueId(), finish, total);
        logger.info(String.format("create reader status %s.", readerStatus));
        ORMUtil.batchReplaceInto(readerStatus);
    }

}
