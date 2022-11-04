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
package org.apache.rocketmq.streams.checkpoint.db;

import java.util.List;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.checkpoint.AbstractCheckPointStorage;
import org.apache.rocketmq.streams.common.checkpoint.CheckPoint;
import org.apache.rocketmq.streams.common.checkpoint.CheckPointManager;
import org.apache.rocketmq.streams.common.checkpoint.ISplitOffset;
import org.apache.rocketmq.streams.common.checkpoint.SourceSnapShot;
import org.apache.rocketmq.streams.db.driver.orm.ORMUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @description
 */
public class DBCheckPointStorage extends AbstractCheckPointStorage {

    static final Logger LOGGER = LoggerFactory.getLogger(DBCheckPointStorage.class);
    static final String STORAGE_NAME = "DB";

    public DBCheckPointStorage() {

    }

    @Override
    public String getStorageName() {
        return STORAGE_NAME;
    }

    @Override
    public <T extends ISplitOffset> void save(List<T> checkPointState) {
        LOGGER.info(String.format("save checkpoint size %d", checkPointState.size()));
        ORMUtil.batchReplaceInto(checkPointState);
    }

    @Override
    public void finish() {

    }

    @Override
    //todo
    public ISplitOffset recover(ISource iSource, String queueId) {
        String sourceName = CheckPointManager.createSourceName(iSource, null);
        String key = CheckPointManager.createCheckPointKey(sourceName, queueId);
        String sql = "select * from source_snap_shot where `key` = " + "'" + key + "';";
        SourceSnapShot snapShot = ORMUtil.queryForObject(sql, null, SourceSnapShot.class);

        LOGGER.info(String.format("checkpoint recover key is %s, sql is %s, recover sourceSnapShot : %s", key, sql, snapShot == null ? "null snapShot" : snapShot.toString()));
        return new CheckPoint().fromSnapShot(snapShot);
    }
}
