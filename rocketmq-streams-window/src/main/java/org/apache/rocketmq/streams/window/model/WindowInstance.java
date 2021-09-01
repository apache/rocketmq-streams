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
package org.apache.rocketmq.streams.window.model;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.model.Entity;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.SQLUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.db.driver.orm.ORMUtil;
import org.apache.rocketmq.streams.window.operator.AbstractWindow;
import org.apache.rocketmq.streams.window.sqlcache.SQLCache;
import org.apache.rocketmq.streams.window.sqlcache.impl.SQLElement;

/**
 * 具体的窗口实例
 */
public class WindowInstance extends Entity implements Serializable {

    protected static final Log LOG = LogFactory.getLog(WindowInstance.class);

    private static final long serialVersionUID = 6893491128670330569L;

    /**
     * 窗口实例的开始时间
     */
    protected String startTime;

    /**
     * 窗口实例的结束时间
     */
    protected String endTime;

    /**
     * fire!
     */
    protected String fireTime;

    /**
     * 使用configName
     */
    protected String windowName;

    protected String splitId;
    /**
     * namespace
     */
    protected String windowNameSpace;
    protected String windowInstanceName;//默认等于窗口名，需要区分不同窗口时使用

    /**
     * splitId,windowNameSpace,windowName,windowInstanceName,windowInstanceName 数据库中存储的是MD5值
     */
    protected String windowInstanceSplitName;
    /**
     * windowInstanceId, splitId,windowNameSpace,windowName,windowInstanceName,windowInstanceName,startTime,endTime" 数据库中存储的是MD5值
     */
    protected String windowInstanceKey;

    protected transient Boolean isNewWindowInstance = false;//当第一次创建时设置为true，否则设置为false

    /**
     * 0：待计算；1：已经计算结束；-1：已经取消;
     */
    protected int status = 0;

    //todo 建议之后改个名字，为了测试方便，暂时用这个字段
    protected Integer version = 1;//用于标识channel的状态，如果值是1，表示第一次消费，否则是第二次消费

    private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private static final String SEPARATOR = "_";

    protected transient Long lastMaxUpdateTime;//last max update time for session window

    /**
     * 创建window instance的唯一ID
     *
     * @return
     */
    public String createWindowInstanceId() {
        return MapKeyUtil.createKey(splitId, windowNameSpace, windowName, windowInstanceName, startTime, endTime);
    }

    public String createWindowInstanceTriggerId(){
        return MapKeyUtil.createKey(splitId, windowNameSpace, windowName, windowInstanceName, startTime, endTime,fireTime);
    }

    /**
     * 创建window instance对象列表
     *
     * @param window
     * @param startAndEndTimeList
     * @param fireTimeList
     * @return
     */
    public static List<WindowInstance> createWindowInstances(AbstractWindow window,
                                                             List<Pair<String, String>> startAndEndTimeList, List<String> fireTimeList, String queueId) {
        List<WindowInstance> lostInstanceList = new ArrayList<>();
        for (int index = 0; index < startAndEndTimeList.size(); index++) {
            Pair<String, String> pair = startAndEndTimeList.get(index);
            WindowInstance windowInstance = window.createWindowInstance(pair.getLeft(), pair.getRight(), fireTimeList.get(index), queueId);

            lostInstanceList.add(windowInstance);
        }
        return lostInstanceList;
    }

    public String createWindowInstancePartitionId() {
        return StringUtil.createMD5Str(MapKeyUtil.createKey(windowNameSpace, windowName, windowInstanceName, startTime, endTime, splitId));
    }

    /**
     * 触发时间比lastTime小的所有的有效的instance
     *
     * @param
     * @return
     */
    public static List<WindowInstance>  queryAllWindowInstance(String lastTime, AbstractWindow window,
                                                              Collection<String> splitIds) {
        if (window.isLocalStorageOnly() || splitIds == null) {
            return null;
        }
        List<String> splitIdList = new ArrayList<>();
        splitIdList.addAll(splitIds);
        String[] splitNames = new String[splitIds.size()];
        for (int i = 0; i < splitNames.length; i++) {
            splitNames[i] = MapKeyUtil.createKey(window.getNameSpace(), window.getConfigureName(), splitIdList.get(i));
            splitNames[i] = StringUtil.createMD5Str(splitNames[i]);
        }
        String sql = "select * from window_instance where "
            + " status =0 and window_instance_split_name in(" + SQLUtil.createInSql(splitNames) + ")";

        List<WindowInstance> dbWindowInstanceList = null;
        try {
            dbWindowInstanceList = ORMUtil.queryForList(sql, null, WindowInstance.class);
        } catch (Exception e) {
            LOG.error("failed in getting unfired window instances", e);
        }
        return dbWindowInstanceList;
    }

    /**
     * 清理window
     *
     * @param windowInstance
     */
    @Deprecated
    public static void cleanWindow(WindowInstance windowInstance) {
        clearInstance(windowInstance,null);
    }
    public static void clearInstance(WindowInstance windowInstance) {
        clearInstance(windowInstance,null);

    }
    public static void clearInstance(WindowInstance windowInstance, SQLCache sqlCache) {
        if (windowInstance==null) {
            return;
        }

        String deleteInstanceById = "delete from " + ORMUtil.getTableName(WindowInstance.class)
            + " where window_instance_key ='" + windowInstance.getWindowInstanceKey() + "'";
        if(sqlCache!=null){
            sqlCache.addCache(new SQLElement(windowInstance.getSplitId(),windowInstance.createWindowInstanceId(),deleteInstanceById));
        }else {
            ORMUtil.executeSQL(deleteInstanceById, null);
        }

    }

    public static Long getOccurTime(AbstractWindow window, IMessage message) {
        Long occurTime = null;
        if (StringUtil.isEmpty(window.getTimeFieldName())) {
            occurTime = message.getMessageBody().getLong("time");
            if (occurTime == null) {
                occurTime = message.getHeader().getSendTime();
            }
        } else {
            try {
                occurTime = message.getMessageBody().getLong(window.getTimeFieldName());
            } catch (Exception e) {
                String occurTimeString = message.getMessageBody().getString(window.getTimeFieldName());
                try {
                    occurTime = dateFormat.parse(occurTimeString).getTime();
                } catch (ParseException parseException) {
                    throw new RuntimeException("can not parse the time field (" + window.getTimeFieldName() + ")");
                }
            }
        }
        if (occurTime == null) {
            throw new RuntimeException("can not parse the time field (" + window.getTimeFieldName() + ")");
        }
        return occurTime;
    }

    /**
     * 查询或者创建Window的实例，滑动窗口有可能返回多个，滚动窗口返回一个
     *
     * @param window
     * @param occurTime
     * @return
     * @Param isWindowInstance2DB 如果是秒级窗口，可能windowinstacne不必存表，只在内存保存，可以通过这个标志设置
     */
    public static List<WindowInstance> getOrCreateWindowInstance(AbstractWindow window, Long occurTime, int timeUnitAdjust, String queueId) {
        int windowSlideInterval = window.getSlideInterval();
        int windowSizeInterval = window.getSizeInterval();
        if (windowSlideInterval == 0) {
            windowSlideInterval = windowSizeInterval;
        }
        int waterMarkMinute = window.getWaterMarkMinute();
        List<Date> windowBeginTimeList = DateUtil.getWindowBeginTime(occurTime, windowSlideInterval * timeUnitAdjust * 1000,
            windowSizeInterval * timeUnitAdjust * 1000);
        List<WindowInstance> instanceList = new ArrayList<>();
        List<Pair<String, String>> lostWindowTimeList = new ArrayList<>();
        List<String> lostFireList = new ArrayList<>();

        Long maxEventTime =window.getMaxEventTime(queueId);
        for (Date begin : windowBeginTimeList) {
            Date end = DateUtil.addDate(TimeUnit.SECONDS, begin, windowSizeInterval * timeUnitAdjust);
            Date fire = null;
            if (window.getFireMode() != 0) {
                //非正常触发模式
                if (maxEventTime==null||maxEventTime - end.getTime() < 0) {
                    fire = end;
                } else {
                    Long nowEventTime =maxEventTime;
                    List<Date> currentWindowList = DateUtil.getWindowBeginTime(
                        nowEventTime, windowSlideInterval * timeUnitAdjust * 1000,
                        windowSizeInterval * timeUnitAdjust * 1000);
                    if (!CollectionUtil.isEmpty(currentWindowList)) {
                        Date soonBegin = currentWindowList.get(currentWindowList.size() - 1);
                        Date soonEnd = DateUtil.addDate(TimeUnit.SECONDS, soonBegin,
                            windowSizeInterval * timeUnitAdjust);
                        Date soonFire = soonEnd;
                        fire = soonFire;
                    }
                    // System.out.println(DateUtil.format(fire));
                    if (fire.getTime() - end.getTime() - waterMarkMinute * timeUnitAdjust * 1000 > 0) {
                        //超过最大watermark，消息需要丢弃
                        break;
                    }
                }
                /**
                 * mode 2 clear window instance in first create window instance
                 */
                if(window.getFireMode()==2&&fire.getTime()==end.getTime()&&waterMarkMinute>0){
                    Date clearWindowInstanceFireTime=DateUtil.addDate(TimeUnit.SECONDS,end, waterMarkMinute * timeUnitAdjust);
                    WindowInstance lastWindowInstance=window.createWindowInstance(DateUtil.format(begin), DateUtil.format(end),DateUtil.format(clearWindowInstanceFireTime) , queueId);
                    window.getWindowInstanceMap().putIfAbsent(lastWindowInstance.createWindowInstanceTriggerId(),lastWindowInstance);
                    window.getSqlCache().addCache(new SQLElement(queueId,lastWindowInstance.createWindowInstanceId(),ORMUtil.createBatchReplacetSQL(lastWindowInstance)));
                    window.getWindowFireSource().registFireWindowInstanceIfNotExist(lastWindowInstance,window);
                }

            } else {
                fire = DateUtil.addDate(TimeUnit.SECONDS, end, waterMarkMinute * timeUnitAdjust);
                if (maxEventTime!=null&&maxEventTime - fire.getTime() > 0) {
                    LOG.warn("*********************the message is discard, because the fire time is exceed****************** "+DateUtil.format(begin)+"-"+DateUtil.format(end)+"---"+DateUtil.format(fire));
                    break;
                }
            }

            String startTime = DateUtil.format(begin);
            String endTime = DateUtil.format(end);
            String fireTime = DateUtil.format(fire);
            String windowInstanceTriggerId = window.createWindowInstance(startTime, endTime, fireTime, queueId).createWindowInstanceTriggerId();
            WindowInstance windowInstance = window.getWindowInstanceMap().get(windowInstanceTriggerId);
            if (windowInstance == null) {
                lostWindowTimeList.add(Pair.of(startTime, endTime));
                lostFireList.add(fireTime);
            } else {
                windowInstance.setFireTime(fireTime);
                instanceList.add(windowInstance);
            }
        }
        List<WindowInstance> lostInstanceList = null;
        lostInstanceList = WindowInstance.createWindowInstances(window, lostWindowTimeList, lostFireList, queueId);

        instanceList.addAll(lostInstanceList);
        for (WindowInstance windowInstance : instanceList) {
            window.getWindowInstanceMap().putIfAbsent(windowInstance.createWindowInstanceTriggerId(), windowInstance);
        }

        return instanceList;
    }

    //public WindowInstance copy() {
    //    WindowInstance windowInstance=new WindowInstance();
    //    windowInstance.setNewWindowInstance(this.getNewWindowInstance());
    //    windowInstance.setVersion(this.version);
    //    windowInstance.setStartTime(this.startTime);
    //    windowInstance.setEndTime(this.endTime);
    //    windowInstance.setStatus(this.status);
    //    windowInstance.setWindowNameSpace(this.windowNameSpace);
    //    windowInstance.setWindowName(this.windowName);
    //    windowInstance.setFireTime(this.fireTime);
    //    windowInstance.setWindowInstanceKey(this.windowInstanceKey);
    //    windowInstance.setGmtCreate(this.gmtCreate);
    //    windowInstance.setGmtModified(this.gmtModified);
    //    return windowInstance;
    //}

    //public WindowInstance toMd5Instance() {
    //    WindowInstance instance = copy();
    //    instance.setWindowInstanceKey(StringUtil.createMD5Str(instance.getWindowInstanceKey()));
    //    return instance;
    //}

    //public WindowInstance toOriginInstance(boolean supportOutDate) {
    //    WindowInstance instance = copy();
    //    instance.setWindowInstanceKey(null);
    //    instance.createWindowInstanceId(supportOutDate);
    //    return instance;
    //}

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

    public String getWindowName() {
        return windowName;
    }

    public void setWindowName(String windowName) {
        this.windowName = windowName;
    }

    public String getWindowNameSpace() {
        return windowNameSpace;
    }

    public void setWindowNameSpace(String windowNameSpace) {
        this.windowNameSpace = windowNameSpace;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public Integer getVersion() {
        return version;
    }

    public void setVersion(Integer version) {
        this.version = version;
    }

    public String getWindowInstanceKey() {
        return windowInstanceKey;
    }

    public String getWindowInstanceName() {
        return windowInstanceName;
    }

    public void setWindowInstanceName(String windowInstanceName) {
        this.windowInstanceName = windowInstanceName;
    }

    public void setWindowInstanceKey(String windowInstanceKey) {
        this.windowInstanceKey = windowInstanceKey;
    }

    public Boolean isNewWindowInstance() {
        return isNewWindowInstance;
    }

    public void setNewWindowInstance(Boolean newWindowInstance) {
        isNewWindowInstance = newWindowInstance;
    }

    public String getSplitId() {
        return splitId;
    }

    public void setSplitId(String splitId) {
        this.splitId = splitId;
    }

    public String getWindowInstanceSplitName() {
        return windowInstanceSplitName;
    }

    public void setWindowInstanceSplitName(String windowInstanceSplitName) {
        this.windowInstanceSplitName = windowInstanceSplitName;
    }

    public Long getLastMaxUpdateTime() {
        return lastMaxUpdateTime;
    }

    public void setLastMaxUpdateTime(Long lastMaxUpdateTime) {
        this.lastMaxUpdateTime = lastMaxUpdateTime;
    }

    @Override
    public int hashCode() {
        return createWindowInstanceId().hashCode();
    }

    @Override public String toString() {
        return createWindowInstanceId().toString();
    }
}
