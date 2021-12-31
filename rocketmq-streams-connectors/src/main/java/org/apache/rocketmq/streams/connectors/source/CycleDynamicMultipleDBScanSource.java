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

import com.alibaba.fastjson.JSONObject;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.channel.source.AbstractSource;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.channel.source.systemmsg.ChangeTableNameMessage;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.metadata.MetaDataUtils;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.ThreadUtil;
import org.apache.rocketmq.streams.connectors.IBoundedSource;
import org.apache.rocketmq.streams.connectors.model.ReaderStatus;
import org.apache.rocketmq.streams.connectors.reader.ISplitReader;
import org.apache.rocketmq.streams.connectors.source.filter.CycleSchedule;
import org.apache.rocketmq.streams.connectors.source.filter.CycleScheduleFilter;
import org.apache.rocketmq.streams.db.CycleSplit;

/**
 * @description
 */
public class CycleDynamicMultipleDBScanSource extends DynamicMultipleDBScanSource implements IBoundedSource, Serializable {

    private static final long serialVersionUID = 6840988298037061128L;
    private static final Log logger = LogFactory.getLog(CycleDynamicMultipleDBScanSource.class);

    Map<String, Boolean> initReaderMap = new ConcurrentHashMap<>();
    CycleSchedule.Cycle cycle;
    transient AtomicInteger size = new AtomicInteger(0);

    public CycleDynamicMultipleDBScanSource() {
        super();
    }

    public CycleDynamicMultipleDBScanSource(CycleSchedule.Cycle cycle) {
        super();
        this.cycle = cycle;
    }

    public AtomicInteger getSize() {
        return size;
    }

    public void setSize(AtomicInteger size) {
        this.size = size;
    }

    /**
     * @return
     */
    //todo
    @Override
    public synchronized List<ISplit> fetchAllSplits() {

        if (this.filter == null) {
            filter = new CycleScheduleFilter(cycle.getAllPattern());
        }

        //如果还是当前周期, 已经完成全部分区的加载, 则不在加载
        if (size.get() == cycle.getCycleCount()) {
            return splits;
        }
        String sourceName = createKey(this);
        List<String> tableNames = MetaDataUtils.listTableNameByPattern(url, userName, password, logicTableName + "%");

        logger.info(String.format("load all logic table : %s", Arrays.toString(tableNames.toArray())));
        Iterator<String> it = tableNames.iterator();
        while (it.hasNext()) {
            String s = it.next();
            String suffix = s.replace(logicTableName + "_", "");
            if (filter.filter(sourceName, logicTableName, suffix)) {
                logger.info(String.format("filter add %s", s));
                CycleSplit split = new CycleSplit();
                split.setLogicTableName(logicTableName);
                split.setSuffix(suffix);
                split.setCyclePeriod(cycle.getCycleDateStr());
                String splitId = split.getQueueId();
                if (initReaderMap.get(splitId) == null) {
                    initReaderMap.put(splitId, false);
                    splits.add(split);
                    size.incrementAndGet();
                }
            } else {
                logger.info(String.format("filter remove %s", s));
                it.remove();
            }
        }

        this.tableNames = tableNames;
        return splits;
    }

    public Map<String, Boolean> getInitReaderMap() {
        return initReaderMap;
    }

    public void setInitReaderMap(Map<String, Boolean> initReaderMap) {
        this.initReaderMap = initReaderMap;
    }

    @Override
    public void finish() {
        super.finish();
        for (Map.Entry<String, Boolean> entry : initReaderMap.entrySet()) {
            String key = entry.getKey();
            Boolean value = entry.getValue();
            if (value == false) {
                logger.error(String.format("split[%s] reader is not finish, exit with error. ", key));
            }
        }
        this.initReaderMap.clear();
        this.initReaderMap = null;
        splits.clear();
        splits = null;
    }

    @Override
    public boolean isFinished() {
        List<ReaderStatus> readerStatuses = ReaderStatus.queryReaderStatusListBySourceName(createKey(this));
        if (readerStatuses == null) {
            return false;
        }
        return readerStatuses.size() == size.get();
    }

    @Override
    protected ISplitReader createSplitReader(ISplit iSplit) {
        return super.createSplitReader(iSplit);
    }

    private void sendChangeTableNameMessage() {
        logger.info(String.format("start send change table name message."));
        ChangeTableNameMessage changeTableNameMessage = new ChangeTableNameMessage();
        changeTableNameMessage.setScheduleCycle(cycle.getCycleDateStr());
        Message message = createMessage(new JSONObject(), null, null, false);
        message.setSystemMessage(changeTableNameMessage);
        message.getHeader().setSystemMessage(true);
        executeMessage(message);
        logger.info(String.format("finish send change table name message."));
    }

    @Override
    public synchronized void boundedFinishedCallBack(ISplit iSplit) {
        this.initReaderMap.put(iSplit.getQueueId(), true);
        logger.info(String.format("current map is %s, key is %s. ", initReaderMap, iSplit.getQueueId()));
        if (statusCheckerStart.compareAndSet(false, true)) {
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (!isFinished()) {
                        ThreadUtil.sleep(3 * 1000);
                    }
                    logger.info(String.format("source will be closed."));
                    sendChangeTableNameMessage(); //下发修改name的消息
                    ThreadUtil.sleep(1 * 1000);
                    finish();
                }

            });
            thread.setName(createKey(this) + "_callback");
            thread.start();
        }
    }

    public CycleSchedule.Cycle getCycle() {
        return cycle;
    }

    public void setCycle(CycleSchedule.Cycle cycle) {
        this.cycle = cycle;
    }

    @Override
    public String createCheckPointName() {
        return super.createCheckPointName();
    }

    public synchronized int getTotalReader() {
        return size.get();
    }

    public static String createKey(ISource iSource) {
        AbstractSource source = (AbstractSource) iSource;
        CycleSchedule.Cycle cycle = ((CycleDynamicMultipleDBScanSource) iSource).getCycle();
        return MapKeyUtil.createKey(source.getNameSpace(), source.getGroupName(), source.getConfigureName(), source.getTopic(), cycle.getCycleDateStr());
    }

}
