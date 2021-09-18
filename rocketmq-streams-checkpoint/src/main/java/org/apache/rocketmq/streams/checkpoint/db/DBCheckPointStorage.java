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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.checkpoint.AbstractCheckPointStorage;
import org.apache.rocketmq.streams.common.checkpoint.CheckPoint;
import org.apache.rocketmq.streams.common.checkpoint.CheckPointManager;
import org.apache.rocketmq.streams.common.checkpoint.SourceSnapShot;
import org.apache.rocketmq.streams.db.driver.orm.ORMUtil;

import java.util.List;

/**
 * @description
 */
public class DBCheckPointStorage extends AbstractCheckPointStorage {

    static final Log logger = LogFactory.getLog(DBCheckPointStorage.class);

    static final String STORAGE_NAME = "DB";

    public DBCheckPointStorage(){

    }

    @Override
    public String getStorageName() {
        return STORAGE_NAME;
    }

    @Override
    public <T> void save(List<T> checkPointState) {
        logger.info(String.format("save checkpoint size %d", checkPointState.size()));
        ORMUtil.batchReplaceInto(checkPointState);
    }

    @Override
    //todo
    public CheckPoint recover(ISource iSource, String queueId) {
        String sourceName = CheckPointManager.createSourceName(iSource, null);
        String key = CheckPointManager.createCheckPointKey(sourceName, queueId);
        String sql = "select * from source_snap_shot where `key` = " + "'" + key + "';";
        SourceSnapShot snapShot = ORMUtil.queryForObject(sql, null, SourceSnapShot.class);

        logger.info(String.format("checkpoint recover key is %s, sql is %s, recover sourceSnapShot : %s", key, sql, snapShot == null ? "null snapShot" : snapShot.toString()));
        return new CheckPoint().fromSnapShot(snapShot);
    }
}
