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
package org.apache.rocketmq.streams.common.monitor.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.channel.sink.ISink;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.configure.ConfigureFileKey;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.datatype.BooleanDataType;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.datatype.IntDataType;
import org.apache.rocketmq.streams.common.datatype.LongDataType;
import org.apache.rocketmq.streams.common.datatype.StringDataType;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.metadata.MetaDataField;
import org.apache.rocketmq.streams.common.monitor.IMonitor;
import org.apache.rocketmq.streams.common.monitor.MonitorFactory;
import org.apache.rocketmq.streams.common.utils.DataTypeUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;

public class DipperMonitor implements IMonitor {

    private static final Log LOG = LogFactory.getLog(DipperMonitor.class);

    private static final String MONITO_SLOW = "SLOW";
    private static final String MONITO_INFO = "INFO";
    private static final String MONITO_ERROR = "ERROR";
    private static final int MONITOR_SLOW_TIMEOUT = 30;//默认慢查询的输出是30s
    public static final String SLOW_NAME = "slow";//对于执行慢的stage进行记录
    public static final String ERROR_NAME = "error";//对于跑错误的stage进行记录
    protected String info = MONITO_INFO;
    protected String debug = MONITO_ERROR;
    protected String warn = MONITO_SLOW + MONITO_ERROR;
    protected String level = debug;

    protected Integer timeoutSecond = MONITOR_SLOW_TIMEOUT;

    protected long startTime = System.currentTimeMillis();
    protected long endTime;
    protected volatile boolean success = true;//本次监控是否有发生错误的逻辑
    protected Exception e;//如果发生了错误，具体的错误是什么
    protected String[] errorMsgs;//如果发生了错误，具体的错误信息是什么
    protected String name;//监控名，业务方确定，不重复即可

    protected Object value;//执行结果

    protected volatile long cost;//花费时长

    protected volatile JSONObject sampleData;//一些上下文的采样数据，便于出错，或慢的时候做分析

    protected List<Object> contextMsgs = new ArrayList<>();//上下文信息，必要的时候，可以设置上下文信息

    protected List<IMonitor> children = new ArrayList<>();//监控小项，一个监控，可以有多个项目

    protected static String LEVEL = null;
    protected static Integer SLOW_TIMEOUT;
    protected static String monitorOutputLevel = null;

    private String type;

    static {
        /**
         * 根据配置文件，把默认的监控配置读出来，主要是level和slow_timeout
         */
        monitorOutputLevel = ConfigureFileKey.MONITOR_OUTPUT_LEVEL;
        LEVEL = ComponentCreator.getProperties().getProperty(monitorOutputLevel);
        String timeoutStr = ComponentCreator.getProperties().getProperty(ConfigureFileKey.MONITOR_SLOW_TIMEOUT);
        if (StringUtil.isNotEmpty(timeoutStr)) {
            SLOW_TIMEOUT = Integer.valueOf(timeoutStr);
        }
    }

    public DipperMonitor() {
        /**
         * 可以在配置文件通过默认配置加.name的方式覆盖默认配置。这里是读取自定义配置，如果没有则用通用配置，通用配置也没有，用默认配置
         */
        if (StringUtil.isNotEmpty(LEVEL)) {
            this.level = LEVEL;
        }

        if (SLOW_TIMEOUT != null) {
            this.timeoutSecond = SLOW_TIMEOUT;
        }
    }

    private DipperMonitor(DipperMonitor parent) {
        this.level = parent.level;
        this.timeoutSecond = parent.timeoutSecond;
    }

    protected void initProperty() {
        String selfMonitorOutputLevel = monitorOutputLevel + "." + name;
        String level = ComponentCreator.getProperties().getProperty(selfMonitorOutputLevel);
        if (StringUtil.isNotEmpty(level)) {
            this.level = level;
        }

        String timeoutStr = ComponentCreator.getProperties().getProperty(ConfigureFileKey.MONITOR_SLOW_TIMEOUT + "." + name);
        if (StringUtil.isNotEmpty(timeoutStr)) {
            this.timeoutSecond = Integer.valueOf(timeoutStr);
        }
    }

    @Override
    public IMonitor createChildren(String... childrenName) {
        String name = MapKeyUtil.createKeyBySign(".", childrenName);
        IMonitor childrenMonitor = new DipperMonitor(this);
        childrenMonitor.startMonitor(name);

        children.add(childrenMonitor);
        return childrenMonitor;
    }

    @Override
    public IMonitor createChildren(IConfigurable configurable) {
        String name = MapKeyUtil.createKeyBySign(".", configurable.getType(), configurable.getNameSpace(), configurable.getConfigureName());
        return createChildren(name);
    }

    @Override
    public IMonitor startMonitor(String name) {
        this.name = name;
        this.startTime = System.currentTimeMillis();
        initProperty();
        return this;
    }

    @Override
    public IMonitor endMonitor() {
        this.endTime = System.currentTimeMillis();
        if (e != null) {
            return this;
        }
        this.success = true;
        this.cost = endTime - startTime;
        return this;
    }

    @Override
    public boolean isSlow() {
        return (cost - timeoutSecond * 1000 > 0);
    }

    @Override
    public boolean isError() {
        return !this.success;
    }

    @Override
    public long getCost() {
        return cost;
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public void setType(String type) {
        this.type = type;
    }

    @Override
    public IMonitor occureError(Exception e, String... messages) {
        endMonitor();
        this.success = false;
        if (this.e != null && (e == null || "null".equals(e))) {
            return this;
        }
        this.e = e;
        //注释掉降低日志量
        //        System.out.println(e.getMessage());
        this.endTime = System.currentTimeMillis();
        this.errorMsgs = messages;
        return this;
    }

    @Override
    public IMonitor addContextMessage(Object value) {
        if (value == null) {
            return this;
        }
        if (IMessage.class.isInstance(value)) {
            JSONObject msgContext = new JSONObject();
            IMessage message = (IMessage)value;
            msgContext.put("orig_msg", message.getMessageBody());
            msgContext.put("orig_header", message.getHeader().toJsonObject());
            this.contextMsgs.add(msgContext);
        }
        this.contextMsgs.add(value);
        return this;
    }

    @Override
    public IMonitor setResult(Object value) {
        this.value = value;
        return this;
    }

    protected JSONObject createErrorJson() {
        JSONObject result = null;
        if (this.success == false) {
            result = new JSONObject();
            //如果有错误，把错误信息记录下来
            JSONArray errorMsgtmp = new JSONArray();
            for (String errorMsg : this.errorMsgs) {
                if (errorMsg != null) {
                    errorMsgtmp.add(errorMsg);
                }
            }
            if (this.e != null) {
                errorMsgtmp.add(e.toString());
                e.printStackTrace();
            }
            if (errorMsgtmp.size() > 0) {
                result.put(MONITOR_ERROR_MSG, errorMsgtmp);
            }

        }
        return result;
    }

    //    protected JSONObject createChildren(){
    //        JSONObject result=null;
    //        if(this.children!=null&&this.children.size()>0){
    //            //如果有子记录，则把子记录记录下来
    //            result=new JSONObject();
    //            JSONArray jsonArray=new JSONArray();
    //            for(int i=0;i<this.children.size();i++){
    //                jsonArray.add(this.children.get(i).report());
    //            }
    //            result.put(IMonitor.MONITOR_CHILDREN,jsonArray);
    //        }
    //        return result;
    //    }

    protected JSONObject createContext() {
        JSONObject result = null;
        if (this.contextMsgs != null && this.contextMsgs.size() > 0) {
            //如果有上下文信息，则把上下文放进去
            result = new JSONObject();
            JSONArray jsonArray = new JSONArray();
            for (int i = 0; i < contextMsgs.size(); i++) {
                jsonArray.add(contextMsgs.get(i));
            }
            result.put(MONITOR_CONTEXT_MSG, jsonArray);
        }
        return result;
    }

    @Override
    public JSONObject report(String level) {
        JSONObject result = new JSONObject();
        result.put(MONTIOR_NAME, name);
        result.put(MONITOR_SUCCESS, success);
        result.put(MONITOR_COST, cost);

        //只输出到慢日志文件
        if (level.indexOf(MONITO_SLOW) != -1) {
            result.put(MONTIOR_SLOW, isSlow());
            Object object = this.getValue();
            if (object != null) {
                if (JSONObject.class.isInstance(object)) {
                    result.put(MONITOR_RESULT, object);
                } else {
                    DataType dataType = DataTypeUtil.getDataTypeFromClass(object.getClass());
                    result.put(MONITOR_RESULT, dataType.toDataJson(object));
                }
            }
        }
        //只输出到错误日志文件
        if (level.indexOf(MONITO_ERROR) != -1) {
            JSONObject error = createErrorJson();
            if (error != null) {
                result.put(MONITOR_ERROR_MSG, error);
            }
        }
        //只输出到info日志目录
        if (level.indexOf(MONITO_INFO) != -1) {
            JSONObject context = createContext();
            if (context != null) {
                result.put(MONITOR_CONTEXT_MSG, context);
            }
            if (sampleData != null) {
                result.put(MONITOR_SAMPLE_DATA, sampleData);
            }
            result.put(MONTIOR_SLOW, isSlow());
            Object object = this.getValue();
            if (object != null) {
                if (JSONObject.class.isInstance(object)) {
                    result.put(MONITOR_RESULT, object);
                } else {
                    DataType dataType = DataTypeUtil.getDataTypeFromClass(object.getClass());
                    result.put(MONITOR_RESULT, dataType.toDataJson(object));
                }
            }
        }
        //已经在finishMonitor方法中做了递归，不需要再次输出
        //        JSONObject children=createChildren();
        //        if(children!=null){
        //            result.put(IMonitor.MONITOR_CHILDREN,children);
        //        }
        return result;
    }

    /**
     * 输出时会用到，比如输出到数据库，则每个字段名，需要和metadata一致
     */
    private static MetaData metaData = new MetaData();

    static {
        metaData.setTableName("monitor_data");
        metaData.setIdFieldName("id");
        metaData.getMetaDataFields().add(createMetaDataField("id", new LongDataType()));
        metaData.getMetaDataFields().add(createMetaDataField(MONITOR_CHILDREN));
        metaData.getMetaDataFields().add(createMetaDataField(MONITOR_CONTEXT_MSG));
        metaData.getMetaDataFields().add(createMetaDataField(MONITOR_COST, new IntDataType()));
        metaData.getMetaDataFields().add(createMetaDataField(MONITOR_ERROR_MSG));
        metaData.getMetaDataFields().add(createMetaDataField(MONITOR_SAMPLE_DATA));
        metaData.getMetaDataFields().add(createMetaDataField(MONITOR_SUCCESS, new BooleanDataType()));
        metaData.getMetaDataFields().add(createMetaDataField(MONTIOR_SLOW, new BooleanDataType()));
        metaData.getMetaDataFields().add(createMetaDataField(MONTIOR_NAME));

    }

    private static MetaDataField createMetaDataField(String name) {
        return createMetaDataField(name, new StringDataType());
    }

    /**
     * 因为所有字段都是字符类型，
     *
     * @param name
     * @return
     */
    private static MetaDataField createMetaDataField(String name, DataType dataType) {
        MetaDataField metaDataField = new MetaDataField();
        metaDataField.setFieldName(name);
        metaDataField.setIsRequired(false);
        metaDataField.setDataType(dataType);
        return metaDataField;
    }

    @Override
    public void output() {
        String level = this.level.toUpperCase();

        if (level.indexOf(MONITO_INFO) != -1) {
            output2Channel(MONITO_INFO);
        }
        //慢日志文件
        if (level.indexOf(MONITO_SLOW) != -1 && timeoutSecond != null && isSlow()) {
            output2Channel(MONITO_SLOW);
        }
        //错误日志文件
        if (level.indexOf(MONITO_ERROR) != -1 && success == false) {
            output2Channel(MONITO_ERROR);
        }

    }

    /**
     * 1 先取个性化的配置（第一部分是根据名字放进去的，第二部分是default放进去的） 2 放入本机的log
     *
     * @param level
     */
    protected void output2Channel(String level) {
        JSONObject result = report(level);
        List<ISink> outputDataSourceList = MonitorFactory.getOutputDataSource(name, level);

        if (outputDataSourceList == null) {
            outputDataSourceList = new ArrayList<>();
        }
        ISink loggerOutputDataSource = MonitorFactory.createOrGetLogOutputDatasource(
            this.name + "_" + level.toLowerCase());
        if (loggerOutputDataSource != null) {
            outputDataSourceList.add(loggerOutputDataSource);
        } else {
            LOG.error("loggerOutputDataSource is null name=" + name + " level=" + level.toLowerCase());
        }
        for (ISink channel : outputDataSourceList) {
            if (channel == null) {
                LOG.error("channel is null name=" + name + " level=" + level.toLowerCase() + " size" + outputDataSourceList.size());
                continue;
            }
            try {
                if (channel != null) {
                    channel.openAutoFlush();
                }

            } catch (Exception e) {
                LOG.error("openAutoFlush error" + e.getMessage() + channel.getConfigureName() + "" + channel.getClass(), e);
            }
            try {
                if (channel != null) {
                    channel.batchAdd(new Message(result));
                }

            } catch (Exception e) {
                LOG.error("batchAdd error" + e.getMessage() + channel.getConfigureName() + "" + channel.getClass(), e);
            }

        }
    }

    @Override
    public List<IMonitor> getChildren() {
        return this.children;
    }

    public boolean isSuccess() {
        return success;
    }

    public Exception getE() {
        return e;
    }

    public String[] getErrorMsgs() {
        return errorMsgs;
    }

    @Override
    public String getName() {
        return name;
    }

    public JSONObject getSampleData() {
        return sampleData;
    }

    @Override
    public JSONObject setSampleData(AbstractContext context) {
        JSONObject sampleData = getSampleData(context);

        this.sampleData = sampleData;
        return this.sampleData;
    }
    //    protected   static IConfiguableCreatorService creatorService= ReflectUtil.forInstance("com.aliyun.yundun.dipper.siem.developer.CreatorServcie");;

    /**
     * 通过上下文获取采样数据，因为有的数据比较大，只取一条
     *
     * @param context
     * @return
     */
    private JSONObject getSampleData(AbstractContext context) {
        JSONObject jsonObject = new JSONObject();
        ;
        if (context.isSplitModel()) {
            jsonObject = new JSONObject();
            List<IMessage> messages = context.getSplitMessages();
            if (messages == null) {
                return null;
            } else {
                if (messages.size() == 0) {
                    jsonObject = new JSONObject();
                } else {
                    jsonObject.putAll(messages.get(0).getMessageBody());
                }
                jsonObject.put("spiltSize", context.getSplitMessages().size());
            }
            return jsonObject;
        } else {
            return context.getMessage().getMessageBody();
        }
    }

    public String getLevel() {
        return level;
    }

    public Integer getTimeoutSecond() {
        return timeoutSecond;
    }

    public Object getValue() {
        return value;
    }

    public List<Object> getContextMsgs() {
        return contextMsgs;
    }

    public static String getMonitorOutputLevel() {
        return monitorOutputLevel;
    }
}
