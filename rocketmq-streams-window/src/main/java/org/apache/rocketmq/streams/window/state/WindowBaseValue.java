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
package org.apache.rocketmq.streams.window.state;

import java.io.Serializable;
import java.util.Date;
import org.apache.rocketmq.streams.common.model.Entity;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.common.utils.SerializeUtil;

public class WindowBaseValue extends Entity implements Serializable {

    private static final long serialVersionUID = -4985883726971532986L;

    /**
     * 唯一标识一个groupBy的窗口计算数据 创建唯一主键 内存及磁盘里使用(partition,windowNamespace,windowName,startTime,endOrFireTime,groupByValue)的拼接 数据库里用上面拼接字符串的MD5
     */
    protected String msgKey;

    /**
     * 唯一标识一个窗口 内存及磁盘使用(patitionId,windowNamespace,windowName,startTime,endOrFireTime) 数据库里使用上面字符串的MD5
     */
    protected String windowInstanceId;

    /**
     * 分片信息（RocketMQ里是queue）
     */
    protected String partition;

    /**
     * 同一分片同一窗口的自增数据（增加逻辑在业务里，为什么不使用id？）
     */
    protected long partitionNum;

    /**
     * 标识一个分片同一个窗口 内存及磁盘使用(partition,windowNamespace,windowName,windowinstanceName,startTime,endTime,partition) 数据库里使用上面字符串的MD5值
     */
    protected String windowInstancePartitionId;

    /**
     * 窗口实例的开始时间
     */
    protected String startTime;

    /**
     * 窗口实例的结束时间
     */
    protected String endTime;

    /**
     * 窗口实例的触发时间
     */
    protected String fireTime;

    protected Long updateVersion = new Long(0);

    public WindowBaseValue() {
        setGmtCreate(DateUtil.getCurrentTime());
        setGmtModified(DateUtil.getCurrentTime());
    }

    public synchronized long incrementUpdateVersion() {

        updateVersion = updateVersion + 1;
        return updateVersion;
    }

    @Override
    public Date getGmtCreate() {
        return gmtCreate;
    }

    @Override
    public void setGmtCreate(Date gmtCreate) {
        this.gmtCreate = gmtCreate;
    }

    @Override
    public Date getGmtModified() {
        return gmtModified;
    }

    @Override
    public void setGmtModified(Date gmtModified) {
        this.gmtModified = gmtModified;
    }

    public String getMsgKey() {
        return msgKey;
    }

    public void setMsgKey(String msgKey) {
        this.msgKey = msgKey;
    }

    public String getWindowInstanceId() {
        return windowInstanceId;
    }

    public void setWindowInstanceId(String windowInstanceId) {
        this.windowInstanceId = windowInstanceId;
    }

    public String getPartition() {
        return partition;
    }

    public void setPartition(String partition) {
        this.partition = partition;
    }

    public long getPartitionNum() {
        return partitionNum;
    }

    public void setPartitionNum(long partitionNum) {
        this.partitionNum = partitionNum;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    public String getFireTime() {
        return fireTime;
    }

    public void setFireTime(String fireTime) {
        this.fireTime = fireTime;
    }

    public long getUpdateVersion() {
        return updateVersion;
    }

    public void setUpdateVersion(long updateVersion) {
        this.updateVersion = (updateVersion);
    }

    public String getWindowInstancePartitionId() {
        return windowInstancePartitionId;
    }

    public void setWindowInstancePartitionId(String windowInstancePartitionId) {
        this.windowInstancePartitionId = windowInstancePartitionId;
    }

    @Override
    public WindowBaseValue clone() {
        byte[] bytes = SerializeUtil.serialize(this);
        WindowBaseValue clonedValue = SerializeUtil.deserialize(bytes);
        return clonedValue;
    }

}

