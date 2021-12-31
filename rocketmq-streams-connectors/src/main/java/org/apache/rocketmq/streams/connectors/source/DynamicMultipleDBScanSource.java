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
package org.apache.rocketmq.streams.connectors.source;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.metadata.MetaDataUtils;
import org.apache.rocketmq.streams.connectors.reader.DBScanReader;
import org.apache.rocketmq.streams.connectors.reader.ISplitReader;
import org.apache.rocketmq.streams.connectors.source.filter.DataFormatPatternFilter;
import org.apache.rocketmq.streams.connectors.source.filter.PatternFilter;
import org.apache.rocketmq.streams.db.DynamicMultipleDBSplit;

/**
 * @description DynamicMultipleDBScanSource
 */
public class DynamicMultipleDBScanSource extends AbstractPullSource implements Serializable {

    private static final long serialVersionUID = 3987103552547019739L;
    private static final Log logger = LogFactory.getLog(DynamicMultipleDBScanSource.class);
    public static final int DEFAULT_BATCH_SIZE = 50;
    public static final int MAX_BATCH_SIZE = 100;

    String url;
    String userName;
    String password;
    String logicTableName;
    String suffix;
    int batchSize;
    List<String> tableNames;
    List<ISplit> splits;
    transient volatile AtomicBoolean statusCheckerStart = new AtomicBoolean(false);

    //todo
    transient PatternFilter filter;

    public DynamicMultipleDBScanSource() {
        splits = new ArrayList<>();
    }

    @Override
    protected boolean initConfigurable() {
        setTopic(logicTableName);
        return super.initConfigurable();
    }

    @Override
    protected boolean isNotDataSplit(String queueId) {
        return tableNames.contains(queueId);
    }

    @Override
    protected ISplitReader createSplitReader(ISplit split) {

        DBScanReader reader = new DBScanReader();
        reader.setISplit(split);
        reader.setUrl(url);
        reader.setUserName(userName);
        reader.setPassword(password);
        reader.setTableName(String.valueOf(split.getQueue()));
        int local = batchSize <= 0 ? DEFAULT_BATCH_SIZE : batchSize;
        local = local > MAX_BATCH_SIZE ? MAX_BATCH_SIZE : local;
        reader.setBatchSize(local);
        reader.setISource(this);
        logger.info(String.format("create reader for split %s", split.getQueueId()));
        return reader;
    }

    @Override
    public List<ISplit> fetchAllSplits() {

        if (filter == null) {
            filter = new DataFormatPatternFilter();
        }

//        String sourceName = createKey(this);

        tableNames = MetaDataUtils.listTableNameByPattern(url, userName, password, logicTableName + "%");

        logger.info(String.format("load all logic table : %s", Arrays.toString(tableNames.toArray())));

        for (String s : tableNames) {
            String suffix = s.replace(logicTableName + "_", "");
            if (filter.filter(null, logicTableName, suffix)) {
                logger.info(String.format("filter add %s", s));
                DynamicMultipleDBSplit split = new DynamicMultipleDBSplit();
                split.setLogicTableName(logicTableName);
                split.setSuffix(suffix);
                splits.add(split);
            } else {
                logger.info(String.format("filter remove %s", s));
            }

        }
        return splits;
    }

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

    public String getLogicTableName() {
        return logicTableName;
    }

    public void setLogicTableName(String logicTableName) {
        this.logicTableName = logicTableName;
    }

    public String getSuffix() {
        return suffix;
    }

    public void setSuffix(String suffix) {
        this.suffix = suffix;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public List<String> getTableNames() {
        return tableNames;
    }

    public void setTableNames(List<String> tableNames) {
        this.tableNames = tableNames;
    }

    public List<ISplit> getSplits() {
        return splits;
    }

    public void setSplits(List<ISplit> splits) {
        this.splits = splits;
    }

    public PatternFilter getFilter() {
        return filter;
    }

    public void setFilter(PatternFilter filter) {
        this.filter = filter;
    }

}
