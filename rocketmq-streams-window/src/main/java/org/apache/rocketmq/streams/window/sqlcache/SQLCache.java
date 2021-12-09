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

package org.apache.rocketmq.streams.window.sqlcache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.streams.common.channel.sinkcache.IMessageFlushCallBack;
import org.apache.rocketmq.streams.common.channel.sinkcache.impl.AbstractMultiSplitMessageCache;
import org.apache.rocketmq.streams.db.driver.DriverBuilder;
import org.apache.rocketmq.streams.db.driver.JDBCDriver;

/**
 * cache sql， async and batch commit
 */

public class SQLCache extends AbstractMultiSplitMessageCache<ISQLElement> {
    protected Boolean isOpenCache=true;//if false，then execute sql when receive sql
    protected Set<String> firedWindowInstances=new HashSet<>();//fired window instance ，if the owned sqls have not commit， can cancel the sqls
    protected Map<String,Integer> windowInstance2Index=new HashMap<>();//set index to ISQLElement group by window instance

    protected boolean isLocalOnly;

    public SQLCache(boolean isLocalOnly) {
        super(null);
        this.isLocalOnly = isLocalOnly;
        this.flushCallBack = new MessageFlushCallBack(new SQLCacheCallback());
        this.setBatchSize(1000);
        this.setAutoFlushTimeGap(10 * 1000);
        this.setAutoFlushSize(100);
        this.openAutoFlush();
    }

    @Override
    public int addCache(ISQLElement isqlElement) {
        if (isLocalOnly) {
            return 0;
        }
        if (isOpenCache == false) {
            DriverBuilder.createDriver().execute(isqlElement.getSQL());
            return 1;
        }
        if (isqlElement.isFireNotify()) {
            firedWindowInstances.add(isqlElement.getWindowInstanceId());
        } else if (isqlElement.isWindowInstanceSQL()) {
            Integer index = windowInstance2Index.get(isqlElement.getWindowInstanceId());
            if (index == null) {
                index = 0;
            }
            index++;
            isqlElement.setIndex(index);
            windowInstance2Index.put(isqlElement.getWindowInstanceId(), index);
        }

        return super.addCache(isqlElement);
    }

    @Override
    protected String createSplitId(ISQLElement msg) {
        return msg.getQueueId();
    }

    protected AtomicInteger executeSQLCount = new AtomicInteger(0);
    protected AtomicInteger cancelQLCount = new AtomicInteger(0);

    protected class SQLCacheCallback implements IMessageFlushCallBack<ISQLElement> {

        @Override
        public boolean flushMessage(List<ISQLElement> messages) {
            List<String> sqls = new ArrayList<>();

            for (ISQLElement isqlElement : messages) {
                if (isqlElement.isSplitSQL()) {
                    sqls.add(isqlElement.getSQL());
                } else if (isqlElement.isWindowInstanceSQL()) {
                    sqls.add(isqlElement.getSQL());
                } else if (isqlElement.isFireNotify()) {
                    windowInstance2Index.remove(isqlElement.getWindowInstanceId());
                    firedWindowInstances.remove(isqlElement.getWindowInstanceId());

                }
            }
            if (sqls.size() == 0) {
                return true;
            }
            JDBCDriver dataSource = DriverBuilder.createDriver();
            try {
                executeSQLCount.addAndGet(sqls.size());
                dataSource.executSqls(sqls);
                System.out.println("execute sql count is " + executeSQLCount.get() + ";  cancel sql count is " + cancelQLCount.get());
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            } finally {
                if (dataSource != null) {
                    dataSource.destroy();
                }
            }
            return true;
        }
    }
}
