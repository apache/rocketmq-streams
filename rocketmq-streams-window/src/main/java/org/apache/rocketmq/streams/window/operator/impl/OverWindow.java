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
package org.apache.rocketmq.streams.window.operator.impl;

import java.util.Date;
import java.util.List;

import org.apache.rocketmq.streams.common.cache.compress.impl.IntValueKV;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.operator.AbstractWindow;
import org.apache.rocketmq.streams.window.storage.WindowType;

/**
 * 只支持 时间去重的场景，日志是按系统时间顺序，所以不落盘。需要设置groupByFieldName和rowNumerName字段
 */
public class OverWindow extends AbstractWindow {

    private static int MAX_SIZE = 1000000;
    protected transient IntValueKV intValueKV;
    protected transient WindowInstance windowInstance;
    /**
     * 需要把生成的序列号返回设置到message，这个是序列号对应的名字
     */
    protected String rowNumerName;
    protected boolean isReservedOne=false;

    /**
     * 针对这个窗口实例完成计算，实际上是写入了缓存，在flush时完成真正的计算。写入缓存时把上下文（header，windowinstance，window）保存在消息中
     *
     * @param message
     * @param context
     */
    @Override
    public AbstractContext<IMessage> doMessage(IMessage message, AbstractContext context) {
        String key = generateShuffleKey(message);
        createWindowInstanceByDate(new Date());
        Integer value = intValueKV.get(key);
        if (value == null) {
            synchronized (this) {
                value = intValueKV.get(key);
                if (value == null) {
                    value = 1;
                    intValueKV.put(key, value);
                }
            }

        }
        if(isReservedOne){
            if(value>1){
                context.breakExecute();
                return context;
            }
        }
        if(rowNumerName!=null){
            message.getMessageBody().put(rowNumerName, value);
        }

        /**
         * 如果超过最大值，直接归0
         */
        if (intValueKV.getSize() > MAX_SIZE) {
            synchronized (this) {
                if (intValueKV.getSize() > MAX_SIZE) {
                    intValueKV = new IntValueKV(MAX_SIZE);
                }
            }
        }
        return context;
    }

    /**
     * 如果时间内无instance，创建，如果不在现有的instance中，现有的失效，重新创建
     *
     * @param date
     * @return
     */
    protected void createWindowInstanceByDate(Date date) {
        if (windowInstance == null) {
            synchronized (this) {
                if (windowInstance == null) {
                    windowInstance = createWindowInstance(date);
                    intValueKV = new IntValueKV(MAX_SIZE);
                    return;
                }
            }
        }
        String dateStr = DateUtil.format(date);
        if (dateStr.compareTo(windowInstance.getStartTime()) >= 0 && dateStr.compareTo(windowInstance.getEndTime()) <= 0) {
            return;
        } else {
            synchronized (this) {
                windowInstance = createWindowInstance(date);
                intValueKV = new IntValueKV(MAX_SIZE);
            }
        }
    }

    /**
     * 根据日期创建window instance
     *
     * @param date
     * @return
     */
    protected WindowInstance createWindowInstance(Date date) {
        List<Date> instanceStartTimes = DateUtil.getWindowBeginTime(date.getTime(), slideInterval, sizeInterval);
        Date instanceStartTime = instanceStartTimes.get(0);
        WindowInstance windowInstance = new WindowInstance();
        windowInstance.setStartTime(DateUtil.format(instanceStartTime));
        Date endDate = DateUtil.addMinute(instanceStartTime, sizeInterval);
        windowInstance.setEndTime(DateUtil.format(endDate));
        return windowInstance;
    }

    @Override
    public boolean isSynchronous() {
        return true;
    }


    @Override public boolean supportBatchMsgFinish() {
        return false;
    }

    @Override
    protected boolean initConfigurable() {
        return super.initConfigurable();
    }

    @Override
    public int fireWindowInstance(WindowInstance windowInstance) {
        return 0;
    }

    @Override
    public void clearFireWindowInstance(WindowInstance windowInstance) {

    }

    public boolean isReservedOne() {
        return isReservedOne;
    }

    public void setReservedOne(boolean reservedOne) {
        isReservedOne = reservedOne;
    }

    public String getRowNumerName() {
        return rowNumerName;
    }

    public void setRowNumerName(String rowNumerName) {
        this.rowNumerName = rowNumerName;
    }

}
