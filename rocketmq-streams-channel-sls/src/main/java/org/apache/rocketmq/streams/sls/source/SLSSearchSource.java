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
package org.apache.rocketmq.streams.sls.source;

import com.alibaba.fastjson.JSONObject;
import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.LogContent;
import com.aliyun.openservices.log.common.LogItem;
import com.aliyun.openservices.log.common.QueriedLog;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.response.GetLogsResponse;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.streams.common.batchsystem.BatchFinishMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.interfaces.IScheduleExecutor;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.common.utils.RuntimeUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.schedule.ScheduleComponent;

public class SLSSearchSource extends SLSSource {
    protected Long pollingTimeSecond;
    protected String cron;// 定时任务
    protected String slsSQL;//sls sql
    protected boolean isStartNow = true;//是否立刻执行一次
    protected int queryTimeSizeSecond = 60 * 10;//查询时间当前时间-offsetGenerator，到当前时间
    protected boolean isPowerSql = true;//是否启用独享版本
    protected Long lastQueryTime = -1L;//最后查询时间

    protected transient ScheduleComponent scheduleComponent = ScheduleComponent.getInstance();
    protected transient Client client;

    /**
     * 模拟offset生成，递增产生
     */
    protected transient AtomicLong offsetGenerator;

    @Override protected boolean initConfigurable() {
        offsetGenerator = new AtomicLong(System.currentTimeMillis());
        return super.initConfigurable();
    }

    /**
     * 模拟offset生成，递增产生
     */
    @Override public boolean startSource() {
        updateLastQueryTime();
        SLSSearchSource source = this;
        client = new Client(this.getEndPoint(), getAccessId(), getAccessKey());
        scheduleComponent.getService().startSchedule(new IScheduleExecutor() {
            @Override public void doExecute() throws InterruptedException {
                executeSQL(slsSQL);
            }

            @Override public String getConfigureName() {
                return source.getConfigureName();
            }

            @Override public String getNameSpace() {
                return source.getNameSpace();
            }

            @Override public String getType() {
                return source.getType();
            }
        }, cron, isStartNow);
        return false;
    }

    protected void updateLastQueryTime() {
        if (configurableService != null) {
            SLSSearchSource slsSearchSource = (SLSSearchSource) configurableService.refreshConfigurable(this.getType(), this.getConfigureName());
            if (slsSearchSource != null) {
                this.lastQueryTime = slsSearchSource.lastQueryTime;
            }
        }
    }

    protected void executeSQL(String slsSQL) {
        Date now = DateUtil.getCurrentTime();
        Date from = DateUtil.addSecond(now, this.queryTimeSizeSecond);
        if (StringUtil.isNotEmpty(this.cron)) {
            queryFromSLS(slsSQL, from.getTime(), now.getTime());
        }

        if (this.pollingTimeSecond != null) {
            if (this.lastQueryTime == null) {
                queryFromSLS(slsSQL, from.getTime(), now.getTime());
            } else {
                //如果系统挂掉，可以断点续传的方式执行查询
                Date virtualNow = DateUtil.addSecond(new Date(this.lastQueryTime), this.pollingTimeSecond.intValue());
                Date virtualFrom = DateUtil.addSecond(virtualNow, this.queryTimeSizeSecond);
                while (Math.abs(virtualNow.getTime() - now.getTime()) < 1000) {
                    queryFromSLS(slsSQL, virtualFrom.getTime(), virtualNow.getTime());
                    virtualNow = DateUtil.addSecond(virtualNow, pollingTimeSecond.intValue());
                    virtualFrom = DateUtil.addSecond(virtualNow, this.queryTimeSizeSecond);
                }
            }

        }
    }

    protected void queryFromSLS(String sql, Long startTime, Long toTime) {
        try {
            String queueId = RuntimeUtil.getDipperInstanceId();
            GetLogsResponse logsResponse = client.executeLogstoreSql(project, logStore, startTime.intValue(), toTime.intValue(), sql, isPowerSql);
            List<JSONObject> rows = new ArrayList<>();
            for (QueriedLog log : logsResponse.getLogs()) {
                LogItem item = log.GetLogItem();
                for (LogContent content : item.mContents) {
                    JSONObject row = new JSONObject();
                    row.put(content.mKey, content.mValue);
                    rows.add(row);
                }
            }
            if (rows != null) {
                for (JSONObject msg : rows) {
                    doReceiveMessage(msg, false, queueId, offsetGenerator.incrementAndGet() + "");
                }
                sendCheckpoint(queueId);
                executeMessage((Message) BatchFinishMessage.create());
            }
            this.lastQueryTime = System.currentTimeMillis();
            this.update();
        } catch (LogException e) {
            e.printStackTrace();
            throw new RuntimeException("execute sls sql error " + sql, e);
        }
    }

    @Override public boolean supportRemoveSplitFind() {
        return false;
    }

    @Override protected boolean isNotDataSplit(String queueId) {
        return false;
    }

    public String getCron() {
        return cron;
    }

    public void setCron(String cron) {
        this.cron = cron;
    }

    public String getSlsSQL() {
        return slsSQL;
    }

    public void setSlsSQL(String slsSQL) {
        this.slsSQL = slsSQL;
    }

    public boolean isStartNow() {
        return isStartNow;
    }

    public void setStartNow(boolean startNow) {
        isStartNow = startNow;
    }

    public int getQueryTimeSizeSecond() {
        return queryTimeSizeSecond;
    }

    public void setQueryTimeSizeSecond(int queryTimeSizeSecond) {
        this.queryTimeSizeSecond = queryTimeSizeSecond;
    }

    public boolean isPowerSql() {
        return isPowerSql;
    }

    public void setPowerSql(boolean powerSql) {
        isPowerSql = powerSql;
    }

    public Long getLastQueryTime() {
        return lastQueryTime;
    }

    public void setLastQueryTime(Long lastQueryTime) {
        this.lastQueryTime = lastQueryTime;
    }
}
