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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.alibaba.fastjson.JSONObject;

import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.topology.ChainStage.PiplineRecieverAfterCurrentNode;
import org.apache.rocketmq.streams.common.topology.stages.udf.IReducer;
import org.apache.rocketmq.streams.common.topology.stages.udf.IRedurce;
import org.apache.rocketmq.streams.common.utils.Base64Utils;
import org.apache.rocketmq.streams.common.utils.InstantiationUtil;
import org.apache.rocketmq.streams.db.driver.orm.ORMUtil;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;
import org.apache.rocketmq.streams.window.debug.DebugWriter;
import org.apache.rocketmq.streams.window.fire.EventTimeManager;
import org.apache.rocketmq.streams.window.model.FunctionExecutor;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.model.WindowCache;
import org.apache.rocketmq.streams.window.offset.IWindowMaxValueManager;
import org.apache.rocketmq.streams.window.offset.WindowMaxValueManager;
import org.apache.rocketmq.streams.window.source.WindowRireSource;
import org.apache.rocketmq.streams.window.state.impl.WindowValue;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.MessageHeader;
import org.apache.rocketmq.streams.common.topology.ChainStage;
import org.apache.rocketmq.streams.common.topology.builder.IStageBuilder;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.topology.stages.WindowChainStage;
import org.apache.rocketmq.streams.common.topology.model.IWindow;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.parser.imp.FunctionParser;
import org.apache.rocketmq.streams.script.operator.impl.AggregationScript;
import org.apache.rocketmq.streams.script.operator.impl.FunctionScript;
import org.apache.rocketmq.streams.script.service.IScriptExpression;
import org.apache.rocketmq.streams.script.service.IScriptParamter;
import org.apache.rocketmq.streams.script.operator.expression.ScriptExpression;
import org.apache.rocketmq.streams.script.service.IAccumulator;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.window.storage.WindowStorage;

import static java.util.concurrent.CompletableFuture.supplyAsync;

/**
 * window definition in the pipeline, created by user's configure in WindowChainStage
 */
public abstract class AbstractWindow extends BasedConfigurable implements IWindow, IStageBuilder<ChainStage> {

    protected static final Log LOG = LogFactory.getLog(AbstractWindow.class);

    /**
     * tumble or hop window 目前不再使用了
     */
    protected String windowType;

    /**
     * 用消息中的哪个字段做时间字段
     */
    protected String timeFieldName ;

    /**
     * having column in having clause eg: key:'having_sum_0001' value:'having_sum_0001=SUM(OrderPrice)<2000' note: here ignore the logical relation value may be multi expression which split by ${SCRIPT_SPLIT_CHAR} update: change sql(move the function into select clause) to escape function in having clause
     */
    private Map<String, String> havingMap = new HashMap<>(16);

    /**
     * computed column in select clause eg: key:'max_valid_user_fail_host_cnt' value:'max_valid_user_fail_host_cnt=max(base_line_invalid_user_fail_host_cnt)' note: 1) value may be multi expression which split by ${SCRIPT_SPLIT_CHAR} 2) computed column can't be same
     */
    private Map<String, String> selectMap = new HashMap<>(16);

    /**
     * SQL中group by的字段，使用;拼接，如"name;age"
     */
    protected String groupByFieldName ;

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
     * 主要是做兼容，以前设计的窗口时间是分钟为单位，如果有秒作为窗口时间的，通过设置timeUntiAdjust=1来实现。 后续需要调整成直接秒级窗口
     */
    protected  int timeUnitAdjust=60;
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
    protected Long msgMaxGapSecond;

    /**
     * 是否支持过期数据的计算 过期：当前时间大于数据所在窗口的触发时间
     */
    protected int fireMode=0;//0:普通触发,firetime后收到数据丢弃；1:多实例多次独立触发，在watermark时间内，同starttime，endtime创建多个实例，多次触发；2.单实例，多次独立触发，每次触发是最新值

    protected boolean isLocalStorageOnly=false;//是否只用本地存储，可以提高性能，但不保证可靠性
    protected String reduceSerializeValue;//用户自定义的operator的序列化字节数组，做了base64解码
    protected transient IReducer reducer;
    /**
     * the computed column and it's process of computing
     */
    private transient Map<String, List<FunctionExecutor>> columnExecuteMap = new HashMap<>(16);

    /**
     * used in last part to filter and transfer field in case data lost during firing
     */
    protected transient Map<String, String> columnProjectMap = new HashMap<>();

    /**
     * 当前计算节点的PipeLine里的Window实例对象，方便基于时间快速定位 key：namespace;configName(这里理解成windowName);startTime;endTime value：WindowInstance
     */
    protected transient ConcurrentHashMap<String, WindowInstance> windowInstanceMap = new ConcurrentHashMap<>();

    /**
     * 触发窗口后需要执行的逻辑
     */
    protected transient PiplineRecieverAfterCurrentNode fireReceiver;

    /**
     * 全局名称
     */
    protected transient String WINDOW_NAME;

    /**
     * 内部使用,定期检查窗口有没有触发
     */
    //protected transient ScheduledExecutorService fireWindowInstanceChecker =new ScheduledThreadPoolExecutor(3);

    // protected transient ExecutorService deleteService = Executors.newSingleThreadExecutor();

    protected volatile transient WindowCache windowCache;
    protected transient WindowStorage storage;
    protected transient WindowRireSource windowFireSource;

    protected transient EventTimeManager eventTimeManager;

    //create and save window instacne max partitionNum and window max eventTime
    protected transient IWindowMaxValueManager windowMaxValueManager;

    public AbstractWindow() {
        setType(IWindow.TYPE);
    }

    @Override
    protected boolean initConfigurable() {
        boolean success = super.initConfigurable();
        /**
         * 如果没有db配置，不开启远程存储服务
         */
        if(!ORMUtil.hasConfigueDB()){
            isLocalStorageOnly=true;
        }
        AbstractWindow window=this;
        windowCache=new WindowCache(){

            @Override
            protected String generateShuffleKey(IMessage message) {
                return window.generateShuffleKey(message);
            }
        };
        windowCache.init();
        windowCache.openAutoFlush();

        WINDOW_NAME = MapKeyUtil.createKey(getNameSpace(), getConfigureName());
        //fireDelaySecond时间检查一次是否有窗口需要触发
        //fireWindowInstanceChecker.scheduleWithFixedDelay(this, 0, 5, TimeUnit.SECONDS);
        initFunctionExecutor();
        //启动shuffle channel 实现消息shuffle以及接收shuffle消息并处理
        // FireManager.getInstance().startFireCheck();
        if(StringUtil.isNotEmpty(this.reduceSerializeValue)){
            byte[] bytes= Base64Utils.decode(  this.reduceSerializeValue);
            reducer = InstantiationUtil.deserializeObject(bytes);
        }
        eventTimeManager=new EventTimeManager();
        windowMaxValueManager = new WindowMaxValueManager(this);
        return success;
    }

    /**
     * 对于一条消息来说，window 首先需要检查是否有窗口实例，如果没有则创建。如果窗口实例已经超过最大的water mark，数据丢弃，否则进行消息积累 消息会先经历batchAdd 然后flush
     *
     * @param message
     * @param context
     * @return
     */
    @Override
    public AbstractContext<IMessage> doMessage(IMessage message, AbstractContext context) {
        if (StringUtils.isNotEmpty(sizeVariable)) {
            if (message.getMessageBody().containsKey(sizeVariable)) {
                try {
                    this.sizeInterval = sizeAdjust * message.getMessageBody().getInteger(sizeVariable);
                } catch (Exception e) {
                    LOG.error("failed in getting the size value, message = " + message.toString(), e);
                }
            }
        }
        if (StringUtils.isNotEmpty(slideVariable)) {
            if (message.getMessageBody().containsKey(slideVariable)) {
                try {
                    this.slideInterval = slideAdjust * message.getMessageBody().getInteger(slideVariable);
                } catch (Exception e) {
                    LOG.error("failed in getting the slide value, message = " + message.toString(), e);
                }
            }
        }
        //  List<WindowInstance> windowInstanceList = queryOrCreateWindowInstance(message);
        JSONObject msg = message.getMessageBody();
        msg.put(MessageHeader.class.getSimpleName(), message.getHeader());
        msg.put(AbstractWindow.class.getSimpleName(), this);
        eventTimeManager.setSource(message.getHeader().getSource());
        windowCache.batchAdd(message);
        //主要为了在单元测试中，写入和触发一体化使用，无实际意义，不要在业务场景使用这个字段

        // TraceUtil.debug(message.getHeader().getTraceId(), "origin message in", message.getMessageBody().toJSONString());
        return context;

    }

    /*

    public String createWindowInstance(String startTime,String endTime,String fireTime){

    }
    */

    public  WindowInstance createWindowInstance(String startTime, String endTime, String fireTime,String splitId) {
        WindowInstance windowInstance =new WindowInstance();
        windowInstance.setFireTime(fireTime);
        windowInstance.setStartTime(startTime);
        windowInstance.setEndTime(endTime);
        windowInstance.setSplitId(splitId);
        windowInstance.setGmtCreate(new Date());
        windowInstance.setGmtModified(new Date());
        windowInstance.setWindowInstanceName(createWindowInstanceName(startTime,endTime,fireTime));
        windowInstance.setWindowName(getConfigureName());
        windowInstance.setWindowNameSpace(getNameSpace());
        String windowInstanceId =windowInstance.createWindowInstanceId();
        String dbWindowInstanceId = StringUtil.createMD5Str(windowInstanceId);
        windowInstance.setWindowInstanceKey(dbWindowInstanceId);

        windowInstance.setWindowInstanceSplitName(StringUtil.createMD5Str(MapKeyUtil.createKey(getNameSpace(), getConfigureName(),splitId)));
        windowInstance.setNewWindowInstance(true);
        return windowInstance;
    }

    /**
     * 创建window instance name
     *
     * @param startTime
     * @param endTime
     * @param fireTime
     * @return
     */
    public String createWindowInstanceName(String startTime, String endTime, String fireTime){
        return fireMode==0?getConfigureName():fireTime;
    }

    /**
     * 获取这个窗口实例，这个分片最大的序列号，如果是新窗口，从1开始
     *
     * @param instance
     * @param shuffleId
     * @return
     */

    public long incrementAndGetSplitNumber(WindowInstance instance,String shuffleId){
        long maxValue= windowMaxValueManager.incrementAndGetSplitNumber(instance,shuffleId);
        return maxValue;
    }

    public abstract Class getWindowBaseValueClass();

    public abstract int fireWindowInstance(WindowInstance windowInstance,Map<String,String>queueId2Offset) ;

    /**
     * 计算每条记录的group by值，对于groupby分组，里面任何字段不能为null值，如果为null值，这条记录会被忽略
     *
     * @param message
     * @return
     */
    protected String generateShuffleKey(IMessage message){
        if (StringUtil.isEmpty(groupByFieldName)) {
            return null;
        }
        JSONObject msg=message.getMessageBody();
        String[] fieldNames = groupByFieldName.split(";");
        String[] values=new String[fieldNames.length];
        boolean isFirst = true;
        int i=0;
        for (String filedName : fieldNames) {
            if (isFirst) {
                isFirst = false;
            }
            String value = msg.getString(filedName);
            values[i]=value;
            i++;
        }
        return MapKeyUtil.createKey(values);
    }

    public abstract void clearFire(List<WindowInstance> windowInstances);

    public void clearFire(WindowInstance windowInstance){
        if(windowInstance==null){
            return;
        }
        List<WindowInstance>windowInstances=new ArrayList<>();
        windowInstances.add(windowInstance);
        clearFire(windowInstances);
    }

    /**
     * init the function executor TODO: 1) function executor may be parsed in parser module;
     */
    protected void initFunctionExecutor() {
        //
        columnExecuteMap.clear();
        columnProjectMap.clear();
        //
        for (Entry<String, String> entry : selectMap.entrySet()) {
            String computedColumn = entry.getKey();
            columnProjectMap.put(computedColumn, computedColumn);
            String scriptString = entry.getValue();
            if (StringUtil.isEmpty(computedColumn) || StringUtil.isEmpty(scriptString)) {
                LOG.warn(
                    "computed column or it's expression can not be empty! column = " + computedColumn + " expression = "
                        + scriptString);
                continue;
            }
            if (computedColumn.equals(scriptString)) {
                continue;
            }
            //
            LinkedList<FunctionExecutor> scriptExecutorList = new LinkedList<>();
            List<IScriptExpression> functionList = new ArrayList<>();
            try {
                functionList = FunctionParser.getInstance().parse(scriptString);
            } catch (Exception e) {
                LOG.error("failed in parsing script expression = " + scriptString + " window = " + WINDOW_NAME);
                throw new RuntimeException("failed in parsing operator expression = " + scriptString);
            }
            if (CollectionUtil.isNotEmpty(functionList)) {
                StringBuilder scriptBuilder = new StringBuilder();
                for (IScriptExpression expression : functionList) {
                    String functionName = expression.getFunctionName();
                    List<IScriptParamter> scriptParameterList = expression.getScriptParamters();
                    String theScript = expression.getExpressionDescription();
                    IAccumulator director = AggregationScript.getAggregationFunction(functionName);
                    if (director != null) {
                        if (scriptBuilder.length() != 0) {
                            FunctionScript scalarEngine = new FunctionScript(scriptBuilder.toString());
                            scalarEngine.init();
                            scriptExecutorList.add(
                                new FunctionExecutor(computedColumn + "_" + scriptExecutorList.size(), scalarEngine));
                            scriptBuilder = new StringBuilder();
                        }
                        String[] functionParameterNames = scriptParameterList.stream().map(
                            scriptParameter -> scriptParameter.getScriptParameterStr()).collect(Collectors.toList())
                            .toArray(new String[0]);
                        AggregationScript accEngine = new AggregationScript(
                            ((ScriptExpression)expression).getNewFieldName(), functionName,
                            functionParameterNames);
                        accEngine.setDirector(director);
                        scriptExecutorList.add(
                            new FunctionExecutor(computedColumn + "_" + scriptExecutorList.size(), accEngine));
                    } else {
                        scriptBuilder.append(theScript).append(SCRIPT_SPLIT_CHAR);
                    }
                }
                if (scriptBuilder.length() != 0) {
                    FunctionScript scalarEngine = new FunctionScript(scriptBuilder.toString());
                    scalarEngine.init();
                    scriptExecutorList.add(
                        new FunctionExecutor(computedColumn + "_" + scriptExecutorList.size(), scalarEngine));
                }
                columnExecuteMap.put(computedColumn, scriptExecutorList);
            } else {
                LOG.error("parser's result is empty, script expression = " + scriptString + " window = " + WINDOW_NAME);
                throw new RuntimeException("parser's result is empty, operator expression = " + scriptString);
            }
        }
        if (LOG.isDebugEnabled()) {
            Iterator<Entry<String, List<FunctionExecutor>>> iterator = columnExecuteMap.entrySet().iterator();
            LOG.debug("window function execute split as follows:\t");
            while (iterator.hasNext()) {
                Entry<String, List<FunctionExecutor>> entry = iterator.next();
                StringBuilder builder = new StringBuilder();
                for (FunctionExecutor executor : entry.getValue()) {
                    if (executor.getExecutor() instanceof AggregationScript) {
                        builder.append(((AggregationScript)executor.getExecutor()).getFunctionName()).append("\t");
                    } else {
                        builder.append(((FunctionScript)executor.getExecutor()).getScript()).append("\t");
                    }
                }
                LOG.debug(entry.getKey() + " -> " + builder.toString());
            }
        }
    }

    /**
     * 根据消息获取对应的window instance 列表
     *
     * @param message
     * @return
     */
    public List<WindowInstance> queryOrCreateWindowInstance(IMessage message,String queueId) {
        List<WindowInstance> windowInstances=WindowInstance.getOrCreateWindowInstance(this, WindowInstance.getOccurTime(this, message), timeUnitAdjust,
            queueId);
//        if(fireMode==2){
//            if(windowInstances==null){
//                return null;
//            }
//            for(WindowInstance windowInstance:windowInstances){
//                Date endTime=DateUtil.parseTime(windowInstance.getEndTime());
//                Date lastFireTimne=DateUtil.addDate(TimeUnit.SECONDS,endTime,getWaterMarkMinute()*timeUnitAdjust);
//                //if fireMode==2， need clear data in lastFireTime
//                WindowInstance lastClearWindowInstance=createWindowInstance(windowInstance.getStartTime(),windowInstance.getEndTime(),DateUtil.format(lastFireTimne),queueId);
//                getWindowFireSource().registFireWindowInstanceIfNotExist(lastClearWindowInstance,this);
//            }
//        }
        return windowInstances;
    }

    /**
     * 获取window处理的消息中最大的时间
     * @param msg
     * @return
     */
    public void updateMaxEventTime(IMessage msg){
        eventTimeManager.updateEventTime(msg,this);
    }

    public Long getMaxEventTime(String queueId) {
       return this.eventTimeManager.getMaxEventTime(queueId);
    }

    /**
     * 聚合后的数据，继续走规则引擎的规则
     *
     * @param windowValueList
     */
    public void sendFireMessage(List<WindowValue> windowValueList,String queueId) {
        int count = 0;
        List<IMessage> msgs=new ArrayList<>();
        for (WindowValue windowValue : windowValueList) {
            JSONObject message = new JSONObject();

            if(JSONObject.class.isInstance(windowValue.getcomputedResult())){
                message=(JSONObject)windowValue.getcomputedResult();
            }else {
                Iterator<Entry<String, Object>> it = windowValue.iteratorComputedColumnResult();
                while (it.hasNext()) {
                    Entry<String, Object> entry = it.next();
                    message.put(entry.getKey(), entry.getValue());
                }
            }

            Long fireTime=DateUtil.parseTime(windowValue.getFireTime()).getTime();
            long baseTime= 1577808000000L  ;//set base time from 2021-01-01 00:00:00
            int sameFireCount=0;
            if(fireMode!=0){
                Long endTime=DateUtil.parseTime(windowValue.getEndTime()).getTime();
                sameFireCount=(int)((fireTime-endTime)/1000)/sizeInterval*timeUnitAdjust;
                if(sameFireCount>=1){
                    sameFireCount=1;
                }
            }
            //can keep offset in order
            Long offset=((fireTime-baseTime)/1000*10+sameFireCount)*100000000+windowValue.getPartitionNum();
            message.put("windowInstanceId",windowValue.getWindowInstancePartitionId());
            message.put("start_time",windowValue.getStartTime());
            message.put("end_time",windowValue.getEndTime());
            message.put("offset",offset);
            Message newMessage=windowFireSource.createMessage(message,queueId,offset+"",false);
            newMessage.getHeader().setOffsetIsLong(true);
            if (count == windowValueList.size() - 1) {
                newMessage.getHeader().setNeedFlush(true);
            }
            msgs.add(newMessage);
            windowFireSource.executeMessage(newMessage);

            count++;
        }

        if(DebugWriter.getDebugWriter(this.getConfigureName()).isOpenDebug()){
            DebugWriter.getDebugWriter(this.getConfigureName()).writeWindowFire(this,msgs,queueId);
        }
    }

    @Override
    public ChainStage createStageChain(PipelineBuilder pipelineBuilder) {
        pipelineBuilder.addConfigurables(this);
        WindowChainStage windowChainStage = new WindowChainStage();
        windowChainStage.setWindow(this);
        windowChainStage.setNameSpace(getNameSpace());
        return windowChainStage;
    }

    @Override
    public void addConfigurables(PipelineBuilder pipelineBuilder) {

    }

    public void setSizeVariable(String variableName) {
        sizeVariable = variableName;
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

    public ConcurrentHashMap<String, WindowInstance> getWindowInstanceMap() {
        return windowInstanceMap;
    }

    public void setWindowInstanceMap(
        ConcurrentHashMap<String, WindowInstance> windowInstanceMap) {
        this.windowInstanceMap = windowInstanceMap;
    }

    public PiplineRecieverAfterCurrentNode getFireReceiver() {
        return fireReceiver;
    }

    @Override
    public void setFireReceiver(
        PiplineRecieverAfterCurrentNode fireReceiver) {
        this.fireReceiver = fireReceiver;
    }

    @Override
    public boolean isSynchronous() {
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

    public String getReduceSerializeValue() {
        return reduceSerializeValue;
    }

    public void setReduceSerializeValue(String reduceSerializeValue) {
        this.reduceSerializeValue = reduceSerializeValue;
    }

    public IReducer getReducer() {
        return reducer;
    }

    public void setReducer(IReducer reducer) {
        this.reducer = reducer;
        byte[] bytes = InstantiationUtil.serializeObject(reducer);
        this.reduceSerializeValue=Base64Utils.encode(bytes);
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

    @Override
    public WindowCache getWindowCache() {
        return windowCache;
    }

    public WindowStorage getStorage() {
        return storage;
    }

    public WindowRireSource getWindowFireSource() {
        return windowFireSource;
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
}
