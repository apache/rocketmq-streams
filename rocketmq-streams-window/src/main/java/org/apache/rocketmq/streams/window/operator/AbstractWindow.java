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
package org.apache.rocketmq.streams.window.operator;

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.rocketmq.streams.common.channel.sink.ISink;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.configurable.annotation.ConfigurableReference;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.functions.MapFunction;
import org.apache.rocketmq.streams.common.model.Pair;
import org.apache.rocketmq.streams.common.topology.IStageBuilder;
import org.apache.rocketmq.streams.common.topology.IWindow;
import org.apache.rocketmq.streams.common.topology.model.AbstractChainStage;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.db.driver.orm.ORMUtil;
import org.apache.rocketmq.streams.script.operator.expression.ScriptExpression;
import org.apache.rocketmq.streams.script.operator.expression.ScriptParameter;
import org.apache.rocketmq.streams.script.operator.impl.AggregationScript;
import org.apache.rocketmq.streams.script.operator.impl.FunctionScript;
import org.apache.rocketmq.streams.script.parser.imp.FunctionParser;
import org.apache.rocketmq.streams.script.service.IAccumulator;
import org.apache.rocketmq.streams.script.service.IScriptExpression;
import org.apache.rocketmq.streams.script.service.IScriptParamter;
import org.apache.rocketmq.streams.window.fire.EventTimeManager;
import org.apache.rocketmq.streams.window.model.FunctionExecutor;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.offset.IWindowMaxValueManager;
import org.apache.rocketmq.streams.window.offset.WindowMaxValueManager;
import org.apache.rocketmq.streams.window.sqlcache.SQLCache;
import org.apache.rocketmq.streams.window.storage.WindowStorage;
import org.apache.rocketmq.streams.window.trigger.WindowTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * window definition in the pipeline, created by user's configure in WindowChainStage
 */
public abstract class AbstractWindow extends BasedConfigurable implements IWindow, IStageBuilder<AbstractChainStage<?>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractWindow.class);
    public static String WINDOW_START = "window_start";
    public static String WINDOW_END = "window_end";
    public static String WINDOW_TIME = "window_time";

    /**
     * tumble or hop window 目前不再使用了
     */
    protected String windowType;

    /**
     * 用消息中的哪个字段做时间字段
     */
    protected String timeFieldName;
    /**
     * SQL中group by的字段，使用;拼接，如"name;age"
     */
    protected String groupByFieldName;
    protected boolean supportRollup;//Support rollup group calculation
    protected List<String> rollupGroupByFieldNames;
    /**
     * 意义同blink中，允许最晚的消息到达时间，单位是分钟
     */
    protected int waterMarkMinute = 0;
    /**
     * size or step of window, unit: minute
     */
    protected int sizeInterval;
    /**
     * the period of hop window, unit: minute
     */
    protected int slideInterval;
    /**
     * 主要是做兼容，以前设计的窗口时间是分钟为单位，如果有秒作为窗口时间的，通过设置timeUnitAdjust=1来实现。 后续需要调整成直接秒级窗口
     */
    protected int timeUnitAdjust = 60;
    /**
     * the variable name of window size which can be got from message
     */
    protected String sizeVariable;
    /**
     * the coefficient to adjust window size for use minute as unit
     */
    protected Integer sizeAdjust;
    /**
     * the coefficient to adjust window slide for use minute as unit
     */
    protected Integer slideAdjust;
    /**
     * the variable name of window slide which it's value can be got from message
     */
    protected String slideVariable;
    /**
     * 默认为空，窗口的触发类似flink，在测试模式下，因为消息有界，期望当消息发送完成后能触发，可以设置两条消息的最大间隔，超过这个间隔，将直接触发消息
     */
    protected Long msgMaxGapSecond = 60 * 3L;
    protected String havingExpression;
    protected Long emitBeforeValue;//output frequency before window fire
    protected Long emitAfterValue;// output frequency after window fire
    protected Long maxDelay = 60 * 60L;//when emitAfterValue>0, window last delay time after window fired
    /**
     * 是否支持过期数据的计算 过期：当前时间大于数据所在窗口的触发时间
     */
    protected int fireMode = 0;//0:普通触发,firetime后收到数据丢弃；1:多实例多次独立触发，在watermark时间内，同starttime，endtime创建多个实例，多次触发；2.单实例，多次独立触发，每次触发是最新值
    protected boolean isLocalStorageOnly = true;//是否只用本地存储，可以提高性能，但不保证可靠性
    protected boolean isOutputWindowInstanceInfo = false;//output default value :window_start,window_end time
    protected transient MapFunction<JSONObject, Pair<WindowInstance, JSONObject>> mapFunction;
    /**
     * the computed column and it's process of computing
     */
    protected transient Map<String, List<FunctionExecutor>> columnExecuteMap = new HashMap<>(16);
    /**
     * used in last part to filter and transfer field in case data lost during firing
     */
    protected transient Map<String, String> columnProjectMap = new HashMap<>();
    /**
     * 当前计算节点的PipeLine里的Window实例对象
     */
    protected transient ConcurrentHashMap<String, WindowInstance> windowInstanceMap = new ConcurrentHashMap<>();
    /**
     * 全局名称
     */
    protected transient String WINDOW_NAME;
    protected transient boolean isEmptyGroupBy = false;//忽略window_start,window_end
    /**
     * 内部使用,定期检查窗口有没有触发
     */
    //protected transient ScheduledExecutorService fireWindowInstanceChecker =new ScheduledThreadPoolExecutor(3);

    // protected transient ExecutorService deleteService = Executors.newSingleThreadExecutor();

    protected transient WindowStorage storage;
    protected transient SQLCache sqlCache;
    protected transient EventTimeManager eventTimeManager;
    @ConfigurableReference protected ISink<?> contextMsgSink;
    protected transient WindowTrigger windowTrigger;
    //create and save window instacne max partitionNum and window max eventTime
    protected transient IWindowMaxValueManager windowMaxValueManager;
    protected Map<String, String> nonStatisticalFieldNames;// Non statistical calculation field, using in rollup
    protected long updateFlag = 0;
    protected AtomicBoolean hasStart = new AtomicBoolean(false);
    /**
     * having column in having clause eg: key:'having_sum_0001' value:'having_sum_0001=SUM(OrderPrice)<2000' note: here ignore the logical relation value may be multi expression which split by ${SCRIPT_SPLIT_CHAR} update: change sql(move the function into select clause) to escape function in having clause
     */
    private Map<String, String> havingMap = new HashMap<>(16);
    /**
     * computed column in select clause eg: key:'max_valid_user_fail_host_cnt' value:'max_valid_user_fail_host_cnt=max(base_line_invalid_user_fail_host_cnt)' note: 1) value may be multi expression which split by ${SCRIPT_SPLIT_CHAR} 2) computed column can't be same
     */
    private Map<String, String> selectMap = new HashMap<>(16);

    public AbstractWindow() {
        setType(IWindow.TYPE);
    }

    @Override
    public void start() {
        if (hasStart.compareAndSet(false, true)) {
            startWindow();
        }
    }

    @Override protected boolean initConfigurable() {
        initFunctionExecutor();
        if (StringUtil.isEmpty(groupByFieldName) && CollectionUtil.isEmpty(this.rollupGroupByFieldNames)) {
            this.isEmptyGroupBy = true;
        }
        /**
         * 如果没有db配置，不开启远程存储服务
         */
        if (!ORMUtil.hasConfigueDB()) {
            isLocalStorageOnly = true;
        }

        WINDOW_NAME = MapKeyUtil.createKey(getNameSpace(), getName());
        if (StringUtil.isNotEmpty(this.groupByFieldName)) {
            String[] fieldNames = groupByFieldName.split(";");
            boolean isEmpty = true;
            for (String filedName : fieldNames) {
                if (WINDOW_START.equals(filedName)) {
                    this.isOutputWindowInstanceInfo = true;
                    continue;
                }
                if (WINDOW_END.equals(filedName)) {
                    this.isOutputWindowInstanceInfo = true;
                    continue;
                }
                if (WINDOW_TIME.equals(filedName)) {
                    this.isOutputWindowInstanceInfo = true;
                    continue;
                }
                isEmpty = false;
                break;
            }
            if (isEmpty && CollectionUtil.isEmpty(this.rollupGroupByFieldNames)) {
                isEmptyGroupBy = true;
            }
        }

        if (CollectionUtil.isNotEmpty(this.rollupGroupByFieldNames)) {
            this.supportRollup = true;
        }
        return super.initConfigurable();
    }

    protected void startWindow() {
        windowInstanceMap = new ConcurrentHashMap();
        sqlCache = new SQLCache(isLocalStorageOnly);
        eventTimeManager = new EventTimeManager();
        windowMaxValueManager = new WindowMaxValueManager(this, sqlCache);
    }

    public WindowInstance createWindowInstance(String startTime, String endTime, String fireTime, String splitId) {
        WindowInstance windowInstance = new WindowInstance();
        windowInstance.setFireTime(fireTime);
        windowInstance.setStartTime(startTime);
        windowInstance.setEndTime(endTime);
        windowInstance.setSplitId(splitId);
        windowInstance.setGmtCreate(new Date());
        windowInstance.setGmtModified(new Date());
        windowInstance.setWindowInstanceName(createWindowInstanceName(startTime, endTime, fireTime));
        windowInstance.setWindowName(getName());
        windowInstance.setWindowNameSpace(getNameSpace());
        String windowInstanceId = windowInstance.createWindowInstanceId();
        String dbWindowInstanceId = StringUtil.createMD5Str(windowInstanceId);
        windowInstance.setWindowInstanceKey(dbWindowInstanceId);
        if (fireMode == 2) {
            windowInstance.setCanClearResource(false);
        } else {
            windowInstance.setCanClearResource(true);
        }
        windowInstance.setWindowInstanceSplitName(StringUtil.createMD5Str(MapKeyUtil.createKey(getNameSpace(), getName(), splitId)));
        windowInstance.setNewWindowInstance(true);
        return windowInstance;
    }

    public AbstractWindow copy() {
        byte[] bytes = ReflectUtil.serialize(this);
        return (AbstractWindow) ReflectUtil.deserialize(bytes);
    }

    /**
     * 创建window instance name
     *
     * @param startTime
     * @param endTime
     * @param fireTime
     * @return
     */
    public String createWindowInstanceName(String startTime, String endTime, String fireTime) {
        return (fireMode == 0 || fireMode == 2) ? getName() : fireTime;
    }

    /**
     * 获取这个窗口实例，这个分片最大的序列号，如果是新窗口，从1开始
     *
     * @param instance
     * @param shuffleId
     * @return
     */

    public long incrementAndGetSplitNumber(WindowInstance instance, String shuffleId) {
        return windowMaxValueManager.incrementAndGetSplitNumber(instance, shuffleId);
    }

    public abstract Class getWindowBaseValueClass();

    /**
     * 计算每条记录的group by值，对于groupby分组，里面任何字段不能为null值，如果为null值，这条记录会被忽略
     *
     * @param message
     * @return
     */
    @Override public String generateShuffleKey(IMessage message) {
        return generateGroupByValue(message).get(0).getValue();
    }

    /**
     * 计算每条记录的group by值，对于groupby分组，里面任何字段不能为null值，如果为null值，这条记录会被忽略
     *
     * @param message
     * @return
     */
    public List<Pair<String, String>> generateGroupByValue(IMessage message) {
        if (isEmptyGroupBy) {
            List<Pair<String, String>> list = new ArrayList<>();
            list.add(new Pair("globle_window", "globle_window"));
            return list;
        }
        List<Pair<String, String>> groupValus = new ArrayList<>();
        List<String> groupFieldNames = new ArrayList<>();
        if (supportRollup && this.rollupGroupByFieldNames != null) {
            if (StringUtil.isEmpty(this.groupByFieldName)) {
                groupFieldNames = this.rollupGroupByFieldNames;
            } else {
                for (String rollupGroupByFieldName : this.rollupGroupByFieldNames) {
                    groupFieldNames.add(this.groupByFieldName + ";" + rollupGroupByFieldName);
                }
            }

        } else {
            groupFieldNames.add(this.groupByFieldName);
        }
        for (String groupFieldName : groupFieldNames) {
            String groupValue = generateGroupByValue(message, groupFieldName);
            groupValus.add(new Pair<>(groupFieldName, groupValue));
        }
        return groupValus;

    }

    public String generateGroupByValue(IMessage message, String groupByFieldName) {
        if (isEmptyGroupBy) {
            return "globle_window";
        }
        JSONObject msg = message.getMessageBody();
        String[] fieldNames = groupByFieldName.split(";");
        String[] values = new String[fieldNames.length];
        boolean isFirst = true;
        int i = 0;
        for (String filedName : fieldNames) {
            if (WINDOW_START.equals(filedName)) {
                continue;
            }
            if (WINDOW_END.equals(filedName)) {
                continue;
            }
            if (WINDOW_TIME.equals(filedName)) {
                continue;
            }
            if (isFirst) {
                isFirst = false;
            }
            String value = msg.getString(filedName);
            values[i] = value;
            i++;
        }
        return MapKeyUtil.createKey(values);
    }

    /**
     * init the function executor TODO: 1) function executor may be parsed in parser module;
     */
    protected void initFunctionExecutor() {
        //
        columnExecuteMap.clear();
        columnProjectMap.clear();
        //
        Map<String, String> nonStatisticalFieldNames = new HashMap<>();
        for (Entry<String, String> entry : selectMap.entrySet()) {
            String computedColumn = entry.getKey();
            columnProjectMap.put(computedColumn, computedColumn);
            String scriptString = entry.getValue();
            if (StringUtil.isEmpty(computedColumn) || StringUtil.isEmpty(scriptString)) {
                LOGGER.warn("computed column or it's expression can not be empty! column = " + computedColumn + " expression = " + scriptString);
                continue;
            }
            if (computedColumn.equals(scriptString)) {
                nonStatisticalFieldNames.put(computedColumn, computedColumn);
                continue;
            }
            //
            LinkedList<FunctionExecutor> scriptExecutorList = new LinkedList<>();
            List<IScriptExpression> functionList = new ArrayList<>();
            try {
                functionList = FunctionParser.getInstance().parse(scriptString);
            } catch (Exception e) {
                LOGGER.error("failed in parsing script expression = " + scriptString + " window = " + WINDOW_NAME);
                throw new RuntimeException("failed in parsing operator expression = " + scriptString);
            }
            if (CollectionUtil.isNotEmpty(functionList)) {
                boolean hasAggregationScript = false;
                StringBuilder scriptBuilder = new StringBuilder();
                for (IScriptExpression expression : functionList) {
                    String functionName = expression.getFunctionName();
                    List<IScriptParamter> scriptParameterList = expression.getScriptParamters();
                    String theScript = expression.getExpressionDescription();
                    IAccumulator director = AggregationScript.getAggregationFunction(functionName);
                    if (director != null) {
                        hasAggregationScript = true;
                        if (scriptBuilder.length() != 0) {
                            FunctionScript scalarEngine = new FunctionScript(scriptBuilder.toString());
                            scalarEngine.init();
                            scriptExecutorList.add(new FunctionExecutor(computedColumn + "_" + scriptExecutorList.size(), scalarEngine));
                            scriptBuilder = new StringBuilder();
                        }
                        String[] functionParameterNames = scriptParameterList.stream().map(scriptParameter -> scriptParameter.getScriptParameterStr()).collect(Collectors.toList()).toArray(new String[0]);
                        AggregationScript accEngine = new AggregationScript(((ScriptExpression) expression).getNewFieldName(), functionName, functionParameterNames);
                        accEngine.setDirector(director);
                        scriptExecutorList.add(new FunctionExecutor(computedColumn + "_" + scriptExecutorList.size(), accEngine));
                    } else {
                        scriptBuilder.append(theScript).append(SCRIPT_SPLIT_CHAR);
                    }
                }
                if (scriptBuilder.length() != 0) {
                    FunctionScript scalarEngine = new FunctionScript(scriptBuilder.toString());
                    scalarEngine.init();
                    scriptExecutorList.add(new FunctionExecutor(computedColumn + "_" + scriptExecutorList.size(), scalarEngine));
                }
                if (hasAggregationScript == false) {
                    FunctionScript functionScript = new FunctionScript(scriptString);
                    functionScript.init();
                    if (functionScript.getScriptExpressions() != null && functionScript.getScriptExpressions().size() == 1) {
                        IScriptExpression scriptExpression = functionScript.getScriptExpressions().get(0);
                        if (StringUtil.isEmpty(scriptExpression.getFunctionName()) && scriptExpression.getScriptParamters() != null && scriptExpression.getScriptParamters().size() == 1) {
                            if (IScriptParamter.class.isInstance(scriptExpression.getScriptParamters().get(0))) {
                                IScriptParamter scriptParamter = (IScriptParamter) scriptExpression.getScriptParamters().get(0);
                                if (isAssignmentExpression(scriptParamter)) {
                                    nonStatisticalFieldNames.put(scriptParamter.getScriptParameterStr(), computedColumn);
                                }
                            }

                        }
                    }
                }
                this.nonStatisticalFieldNames = nonStatisticalFieldNames;
                columnExecuteMap.put(computedColumn, scriptExecutorList);
            } else {
                LOGGER.error("parser's result is empty, script expression = " + scriptString + " window = " + WINDOW_NAME);
                throw new RuntimeException("parser's result is empty, operator expression = " + scriptString);
            }
        }
        if (LOGGER.isDebugEnabled()) {
            Iterator<Entry<String, List<FunctionExecutor>>> iterator = columnExecuteMap.entrySet().iterator();
            LOGGER.debug("window function execute split as follows:\t");
            while (iterator.hasNext()) {
                Entry<String, List<FunctionExecutor>> entry = iterator.next();
                StringBuilder builder = new StringBuilder();
                for (FunctionExecutor executor : entry.getValue()) {
                    if (executor.getExecutor() instanceof AggregationScript) {
                        builder.append(((AggregationScript) executor.getExecutor()).getFunctionName()).append("\t");
                    } else {
                        builder.append(((FunctionScript) executor.getExecutor()).getScript()).append("\t");
                    }
                }
                LOGGER.debug(entry.getKey() + " -> " + builder.toString());
            }
        }
    }

    protected boolean isAssignmentExpression(IScriptParamter paramter) {
        if (!ScriptParameter.class.isInstance(paramter)) {
            return false;
        }
        ScriptParameter scriptParameter = (ScriptParameter) paramter;
        String fieldName = null;
        if (StringUtil.isEmpty(scriptParameter.getFunctionName()) && StringUtil.isEmpty(scriptParameter.getRigthVarName()) && StringUtil.isNotEmpty(scriptParameter.getLeftVarName())) {
            return true;
        }

        return false;
    }

    /**
     * 根据消息获取对应的window instance 列表
     *
     * @param message
     * @return
     */
    public List<WindowInstance> queryOrCreateWindowInstance(IMessage message, String queueId) {
        return WindowInstance.getOrCreateWindowInstance(this, WindowInstance.getOccurTime(this, message), timeUnitAdjust, queueId);
    }

    public List<WindowInstance> queryOrCreateWindowInstanceOnly(IMessage message, String queueId) {
        return WindowInstance.getOrCreateWindowInstance(this, WindowInstance.getOccurTime(this, message), timeUnitAdjust, queueId, true);
    }

    public WindowInstance registerWindowInstance(WindowInstance windowInstance) {
        return registerWindowInstance(windowInstance.createWindowInstanceTriggerId(), windowInstance);
    }

    /**
     * register window instance with indexId key
     *
     * @param indexId
     * @param windowInstance
     */
    protected WindowInstance registerWindowInstance(String indexId, WindowInstance windowInstance) {
        return windowInstanceMap.putIfAbsent(indexId, windowInstance);
    }

    /**
     * search window instance by using index id, return null if not exist
     *
     * @param indexId
     * @return
     */
    public WindowInstance searchWindowInstance(String indexId) {
        return windowInstanceMap.getOrDefault(indexId, null);
    }

    /**
     * logout window instance by using index id
     *
     * @param indexId
     */
    public void logoutWindowInstance(String indexId) {
        windowInstanceMap.remove(indexId);
    }

    /**
     * 获取window处理的消息中最大的时间
     *
     * @param msg
     * @return
     */
    public void updateMaxEventTime(IMessage msg) {
        eventTimeManager.updateEventTime(msg, this);
    }

    public Long getMaxEventTime(String queueId) {
        return this.eventTimeManager.getMaxEventTime(queueId);
    }

    public String getWindowType() {
        return windowType;
    }

    public void setWindowType(String windowType) {
        this.windowType = windowType;
    }

    public String getTimeFieldName() {
        return timeFieldName;
    }

    public void setTimeFieldName(String timeFieldName) {
        this.timeFieldName = timeFieldName;
    }

    public Map<String, String> getSelectMap() {
        return selectMap;
    }

    public void setSelectMap(Map<String, String> selectMap) {
        this.selectMap = selectMap;
    }

    public Map<String, List<FunctionExecutor>> getColumnExecuteMap() {
        return columnExecuteMap;
    }

    public Map<String, String> getColumnProjectMap() {
        return columnProjectMap;
    }

    public String getGroupByFieldName() {
        return groupByFieldName;
    }

    public void setGroupByFieldName(String groupByFieldName) {
        this.groupByFieldName = groupByFieldName;
    }

    public int getWaterMarkMinute() {
        return waterMarkMinute;
    }

    public void setWaterMarkMinute(int waterMarkMinute) {
        this.waterMarkMinute = waterMarkMinute;
    }

    public int getSizeInterval() {
        return sizeInterval;
    }

    public void setSizeInterval(int sizeInterval) {
        this.sizeInterval = sizeInterval;
    }

    private ConcurrentHashMap<String, WindowInstance> getWindowInstanceMap() {
        return windowInstanceMap;
    }

    private void setWindowInstanceMap(ConcurrentHashMap<String, WindowInstance> windowInstanceMap) {
        this.windowInstanceMap = windowInstanceMap;
    }

    @Override public boolean isSynchronous() {
        return false;
    }

    public Map<String, String> getHavingMap() {
        return havingMap;
    }

    public void setHavingMap(Map<String, String> havingMap) {
        this.havingMap = havingMap;
    }

    public int getSlideInterval() {
        return slideInterval;
    }

    public void setSlideInterval(int slideInterval) {
        this.slideInterval = slideInterval;
    }

    public String getSizeVariable() {
        return sizeVariable;
    }

    public void setSizeVariable(String variableName) {
        sizeVariable = variableName;
    }

    public Integer getSizeAdjust() {
        return sizeAdjust;
    }

    public void setSizeAdjust(Integer sizeAdjust) {
        this.sizeAdjust = sizeAdjust;
    }

    public Integer getSlideAdjust() {
        return slideAdjust;
    }

    public void setSlideAdjust(Integer slideAdjust) {
        this.slideAdjust = slideAdjust;
    }

    public String getSlideVariable() {
        return slideVariable;
    }

    public void setSlideVariable(String slideVariable) {
        this.slideVariable = slideVariable;
    }

    public int getTimeUnitAdjust() {
        return timeUnitAdjust;
    }

    public void setTimeUnitAdjust(int timeUnitAdjust) {
        this.timeUnitAdjust = timeUnitAdjust;
    }

    public boolean isLocalStorageOnly() {
        return isLocalStorageOnly;
    }

    public void setLocalStorageOnly(boolean localStorageOnly) {
        isLocalStorageOnly = localStorageOnly;
    }

    public int getFireMode() {
        return fireMode;
    }

    public void setFireMode(int fireMode) {
        this.fireMode = fireMode;
    }

    public void removeInstanceFromMap(WindowInstance windowInstance) {
        this.windowInstanceMap.remove(windowInstance.createWindowInstanceId());

    }

    public WindowStorage getStorage() {
        return storage;
    }

    public IWindowMaxValueManager getWindowMaxValueManager() {
        return windowMaxValueManager;
    }

    public Long getMsgMaxGapSecond() {
        return msgMaxGapSecond;
    }

    public void setMsgMaxGapSecond(Long msgMaxGapSecond) {
        this.msgMaxGapSecond = msgMaxGapSecond;
    }

    public EventTimeManager getEventTimeManager() {
        return eventTimeManager;
    }

    public SQLCache getSqlCache() {
        return sqlCache;
    }

    public void initWindowInstanceMaxSplitNum(WindowInstance instance) {
        getWindowMaxValueManager().initMaxSplitNum(instance, queryWindowInstanceMaxSplitNum(instance));
    }

    protected abstract Long queryWindowInstanceMaxSplitNum(WindowInstance instance);

    public String getHavingExpression() {
        return havingExpression;
    }

    public void setHavingExpression(String havingExpression) {
        this.havingExpression = havingExpression;
    }

    public Long getEmitBeforeValue() {
        return emitBeforeValue;
    }

    public void setEmitBeforeValue(Long emitBeforeValue) {
        this.emitBeforeValue = emitBeforeValue;
    }

    public Long getEmitAfterValue() {
        return emitAfterValue;
    }

    public void setEmitAfterValue(Long emitAfterValue) {
        this.emitAfterValue = emitAfterValue;
    }

    public Long getMaxDelay() {
        return maxDelay;
    }

    public void setMaxDelay(Long maxDelay) {
        this.maxDelay = maxDelay;
    }

    public abstract boolean supportBatchMsgFinish();

    public ISink getContextMsgSink() {
        return contextMsgSink;
    }

    public void setContextMsgSink(ISink<?> contextMsgSink) {
        this.contextMsgSink = contextMsgSink;
    }

    public void saveMsgContext(String queueId, WindowInstance windowInstance, List<IMessage> messages) {
        if (this.mapFunction != null && this.contextMsgSink != null) {
            if (messages != null) {
                for (IMessage message : messages) {
                    JSONObject msg = message.getMessageBody();
                    try {
                        msg = this.mapFunction.map(new Pair(windowInstance, msg));
                        Message copyMsg = new Message(msg);
                        copyMsg.getHeader().setQueueId(queueId);
                        copyMsg.getHeader().setOffset(message.getHeader().getOffset());
                        this.contextMsgSink.batchAdd(copyMsg);
                    } catch (Exception e) {
                        throw new RuntimeException("save window context msg error ", e);
                    }
                }
                this.contextMsgSink.flush();
            }
        }
    }

    @Override public void destroy() {
        super.destroy();
        hasStart.set(false);
    }

    public MapFunction<JSONObject, Pair<WindowInstance, JSONObject>> getMapFunction() {
        return mapFunction;
    }

    public void setMapFunction(
        MapFunction<JSONObject, Pair<WindowInstance, JSONObject>> mapFunction) {
        this.mapFunction = mapFunction;
    }

    public boolean isOutputWindowInstanceInfo() {
        return isOutputWindowInstanceInfo;
    }

    public void setOutputWindowInstanceInfo(boolean outputWindowInstanceInfo) {
        isOutputWindowInstanceInfo = outputWindowInstanceInfo;
    }

    public boolean isEmptyGroupBy() {
        return isEmptyGroupBy;
    }

    public boolean isSupportRollup() {
        return supportRollup;
    }

    public void setSupportRollup(boolean supportRollup) {
        this.supportRollup = supportRollup;
    }

    public List<String> getRollupGroupByFieldNames() {
        return rollupGroupByFieldNames;
    }

    public void setRollupGroupByFieldNames(List<String> rollupGroupByFieldNames) {
        this.rollupGroupByFieldNames = rollupGroupByFieldNames;
    }

    public Map<String, String> getNonStatisticalFieldNames() {
        return nonStatisticalFieldNames;
    }

    public void setNonStatisticalFieldNames(Map<String, String> nonStatisticalFieldNames) {
        this.nonStatisticalFieldNames = nonStatisticalFieldNames;
    }

    public WindowTrigger getWindowTrigger() {
        return windowTrigger;
    }

    public void setWindowTrigger(WindowTrigger windowTrigger) {
        this.windowTrigger = windowTrigger;
    }

    public long getUpdateFlag() {
        return updateFlag;
    }
}
