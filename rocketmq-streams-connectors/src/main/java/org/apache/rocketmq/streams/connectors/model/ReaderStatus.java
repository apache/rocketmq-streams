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
package org.apache.rocketmq.streams.connectors.model;

import java.util.Date;
import java.util.List;
import org.apache.rocketmq.streams.common.model.Entity;
import org.apache.rocketmq.streams.db.driver.orm.ORMUtil;

/**
 * @description
 */
public class ReaderStatus extends Entity {

    /**
     * 查询单个readerStatus
     */
    static final String queryReaderStatusByUK = "select * from reader_status where source_name = '%s' and reader_name = '%s' and is_finished = 1";

    static final String queryReaderStatusList = "select * from reader_status where source_name = '%s' and is_finished = 1";

    static final String clearReaderStatus = "update reader_status set gmt_modified = now(), is_finished = -1 where source_name = '%s' and reader_name = '%s'";

    String sourceName;

    String readerName;

    int isFinished;

    int totalReader;

    public String getReaderName() {
        return readerName;
    }

    public void setReaderName(String readerName) {
        this.readerName = readerName;
    }

    public int getIsFinished() {
        return isFinished;
    }

    public void setIsFinished(int isFinished) {
        this.isFinished = isFinished;
    }

    public int getTotalReader() {
        return totalReader;
    }

    public void setTotalReader(int totalReader) {
        this.totalReader = totalReader;
    }

    public String getSourceName() {
        return sourceName;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    @Override
    public String toString() {
        return "ReaderStatus{" +
            "id=" + id +
            ", gmtCreate=" + gmtCreate +
            ", gmtModified=" + gmtModified +
            ", sourceName='" + sourceName + '\'' +
            ", readerName='" + readerName + '\'' +
            ", isFinished=" + isFinished +
            ", totalReader=" + totalReader +
            '}';
    }

    public static ReaderStatus queryReaderStatusByUK(String sourceName, String readerName){
        String sql = String.format(queryReaderStatusByUK, sourceName, readerName);
        ReaderStatus readerStatus = ORMUtil.queryForObject(sql, null, ReaderStatus.class);
        return readerStatus;
    }

    public static List<ReaderStatus> queryReaderStatusListBySourceName(String sourceName){
        String sql = String.format(queryReaderStatusList, sourceName);
        List<ReaderStatus> readerStatusList = ORMUtil.queryForList(sql, null, ReaderStatus.class);
        return readerStatusList;
    }

    public static void clearReaderStatus(String sourceName, String readerName){
        String sql = String.format(clearReaderStatus, sourceName, readerName);
        ORMUtil.executeSQL(sql, null);
    }

    public static ReaderStatus create(String sourceName, String readerName, int isFinished, int totalReader){

        ReaderStatus readerStatus = new ReaderStatus();
        readerStatus.setSourceName(sourceName);
        readerStatus.setReaderName(readerName);
        readerStatus.setIsFinished(isFinished);
        readerStatus.setTotalReader(totalReader);
        readerStatus.setGmtCreate(new Date());
        readerStatus.setGmtModified(new Date());
        return readerStatus;

    }
}
