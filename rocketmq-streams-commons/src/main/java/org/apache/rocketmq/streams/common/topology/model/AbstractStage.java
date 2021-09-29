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
package org.apache.rocketmq.streams.common.topology.model;

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;
import org.apache.rocketmq.streams.common.interfaces.ISystemMessageProcessor;
import org.apache.rocketmq.streams.common.optimization.SQLLogFingerprintFilter;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.common.utils.TraceUtil;

public abstract class AbstractStage<T extends IMessage> extends BasedConfigurable
    implements IStreamOperator<T, T>, ISystemMessageProcessor {
    protected String filterFieldNames;
    protected transient AbstractStage sourceStage;

    private static final Log LOG = LogFactory.getLog(AbstractStage.class);

    public static final String TYPE = "stage";

    protected transient String name;

    //是否关闭拆分模式，多条日志会合并在一起，字段名为splitDataFieldName
    protected boolean closeSplitMode = false;

    protected String splitDataFieldName;

    protected transient Pipeline pipeline;

    /**
     * 设置路由label，当需要做路由选择时需要设置
     */
    protected String label;

    /**
     * 如果是拓扑结构，则设置next节点的label name
     */
    protected List<String> nextStageLabels = new ArrayList<>();

    /**
     * 上游对应的label列表
     */
    protected List<String> prevStageLabels = new ArrayList<>();

    /**
     * 消息来源于哪个上游节点，对应的上游是sql node的输出表名，如create view tablename的tablename
     */
    protected String msgSourceName;

    /**
     * 这个stage所属哪段sql，sql的输出表名。如create view tablename的tablename
     */
    protected String ownerSqlNodeTableName;

    public AbstractStage() {
        setType(TYPE);
    }
    protected transient AtomicLong TOTAL=new AtomicLong(0);
    protected transient AtomicLong FILTER=new AtomicLong(0);
    protected transient Long lastUpdateTime=null;
    @Override
    public T doMessage(T t, AbstractContext context) {
        if(this.logFingerFieldNames!=null){
            TOTAL.incrementAndGet();
            if(lastUpdateTime==null){
                lastUpdateTime=System.currentTimeMillis();
            }
            if(TOTAL.get() %1000==0){
                long qps=TOTAL.get()*1000/(System.currentTimeMillis()-lastUpdateTime);
                System.out.println("logfinger filter qps is "+qps+"  the filte rate is "+(double)FILTER.get()*100/(double)TOTAL.get()+"%");
            }
        }
        if (filterByLogFingerprint(t)) {
            FILTER.incrementAndGet();

            context.breakExecute();
            return null;
        }
        try {

            TraceUtil.debug(t.getHeader().getTraceId(), "AbstractStage", label, t.getMessageBody().toJSONString());
        } catch (Exception e) {
            LOG.error("t.getMessageBody() parse error", e);
        }
        IStageHandle handle = selectHandle(t, context);
        if (handle == null) {
            return t;
        }
        Object result = handle.doMessage(t, context);
        //
        if (!context.isContinue() || result == null) {
            return (T)context.breakExecute();
        }
        return (T)result;
    }

    /**
     * 是否是异步节点，流程会在此节点终止，启动新线程开启后续流程
     *
     * @return
     */
    public abstract boolean isAsyncNode();

    /**
     * 复制一个新的stage，主要用于并发场景
     *
     * @return
     */
    // public abstract AbstractStage copy();
    protected abstract IStageHandle selectHandle(T t, AbstractContext context);

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    String toJsonString() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("name", getName());
        return jsonObject.toJSONString();
    }

    public boolean isCloseSplitMode() {
        return closeSplitMode;
    }

    public void setCloseSplitMode(boolean closeSplitMode) {
        this.closeSplitMode = closeSplitMode;
    }

    public String getSplitDataFieldName() {
        return splitDataFieldName;
    }

    public void setSplitDataFieldName(String splitDataFieldName) {
        this.splitDataFieldName = splitDataFieldName;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public List<String> doRoute(T t) {
        String routeLabel = t.getHeader().getRouteLables();
        String filterLabel = t.getHeader().getFilterLables();
        if (StringUtil.isEmpty(routeLabel) && StringUtil.isEmpty(filterLabel)) {
            return this.nextStageLabels;
        }
        List<String> lables = new ArrayList<>();
        lables.addAll(this.nextStageLabels);
        if (StringUtil.isNotEmpty(routeLabel)) {
            lables = new ArrayList<>();
            for (String tempLabel : this.nextStageLabels) {
                if (tempLabel != null && routeLabel.indexOf(tempLabel) != -1) {
                    lables.add(tempLabel);
                }
            }
        }

        if (StringUtil.isNotEmpty(filterLabel)) {
            for (String tempLabel : this.nextStageLabels) {
                if (label != null && filterLabel.indexOf(label) != 1) {
                    lables.remove(tempLabel);
                }
            }
        }
        return lables;
    }

    protected transient String logFingerFieldNames;//如果有日志指纹，这里存储日志指纹的字段，启动时，通过属性文件加载
    protected transient String logFingerFilterStageName;//唯一标识一个filter

    protected transient SQLLogFingerprintFilter logFingerprintFilter;//日志指纹的数据存储

    /**
     * 通过日志指纹过滤，如果有过滤日志指纹字段，做过滤判断
     *
     * @param message
     * @return
     */
    protected boolean filterByLogFingerprint(T message) {
        if (logFingerFieldNames != null) {
            String logFingerValue = createLogFingerValue(message);
            if (logFingerprintFilter != null && logFingerValue != null) {
                Integer value = logFingerprintFilter.getFilterValue(logFingerValue);
                if (value != null && value > 0) {
                    return true;
                } else {
                    message.getHeader().setLogFingerprintValue(logFingerValue);
                }
            }
        }
        return false;
    }

    /**
     * 从配置文件加载日志指纹信息，如果存在做指纹优化
     */
    protected void loadLogFinger() {
        ChainPipeline pipline = (ChainPipeline)getPipeline();
        String filterName = getLabel();
        if (pipline.isTopology() == false) {
            List<AbstractStage> stages = pipline.getStages();
            int i = 0;
            for (AbstractStage stage : stages) {
                if (stage == this) {
                    break;
                }
                i++;
            }
            filterName = i + "";
        }
        String key= MapKeyUtil.createKeyBySign(".", pipline.getNameSpace(), pipline.getConfigureName(), filterName);
        if(this.filterFieldNames==null){
            this.filterFieldNames = ComponentCreator.getProperties().getProperty(key);

        }
        if (this.filterFieldNames == null) {
            return;
        }
        sourceStage = getSourceStage();

        sourceStage.setLogFingerFieldNames(filterFieldNames);
        sourceStage.setLogFingerFilterStageName(key);
        sourceStage.setLogFingerprintFilter(SQLLogFingerprintFilter.getInstance());
    }

    /**
     * 发现最源头的stage
     *
     * @return
     */

    protected AbstractStage getSourceStage() {
        ChainPipeline pipline = (ChainPipeline)getPipeline();
        if (pipline.isTopology()) {
            Map<String, AbstractStage> stageMap = pipline.createStageMap();
            AbstractStage currentStage = this;
            List<String> prewLables = currentStage.getPrevStageLabels();
            while (prewLables != null && prewLables.size() > 0) {
                if (prewLables.size() > 1) {
                    return null;
                }
                String lable = prewLables.get(0);
                AbstractStage stage = (AbstractStage)stageMap.get(lable);
                if (stage != null) {
                    currentStage = stage;
                } else {
                    return currentStage;
                }
                prewLables = currentStage.getPrevStageLabels();
            }
            return currentStage;
        } else {
            return (AbstractStage)pipline.getStages().get(0);
        }
    }
    /**
     * 创建过滤指纹值
     *
     * @return
     */
    protected String createLogFingerValue(T message) {
        if (logFingerprintFilter != null) {
            return logFingerprintFilter.createMessageKey(message, logFingerFieldNames, logFingerFilterStageName);
        }
        return null;
    }
    /**
     * 设置过滤指纹
     *
     * @param message
     */
    public void addLogFingerprintToSource(IMessage message) {
        if(sourceStage!=null){
            sourceStage.addLogFingerprint(message);
        }
    }
    /**
     * 设置过滤指纹
     *
     * @param message
     */
    private void addLogFingerprint(IMessage message) {

        String logFingerValue = message.getHeader().getLogFingerprintValue();
        if (logFingerprintFilter != null && logFingerValue != null) {
            logFingerprintFilter.addNoFireMessage(logFingerValue, logFingerFilterStageName);
        }
    }

    public List<String> getNextStageLabels() {
        return nextStageLabels;
    }

    //    public List<String> getNextStageLables() {
    //        return nextStageLables;
    //    }

    public List<String> getPrevStageLabels() {
        return prevStageLabels;
    }

    public void setPrevStageLabels(List<String> prevStageLabels) {
        this.prevStageLabels = prevStageLabels;
    }

    public String getMsgSourceName() {
        return msgSourceName;
    }

    public void setMsgSourceName(String msgSourceName) {
        this.msgSourceName = msgSourceName;
    }

    public Pipeline getPipeline() {
        return pipeline;
    }

    public void setPipeline(Pipeline pipeline) {
        this.pipeline = pipeline;
    }

    public String getOwnerSqlNodeTableName() {
        return ownerSqlNodeTableName;
    }

    public void setOwnerSqlNodeTableName(String ownerSqlNodeTableName) {
        this.ownerSqlNodeTableName = ownerSqlNodeTableName;
    }

    public void setNextStageLabels(List<String> nextStageLabels) {
        this.nextStageLabels = nextStageLabels;
    }

    public String getLogFingerFieldNames() {
        return logFingerFieldNames;
    }

    public void setLogFingerFieldNames(String logFingerFieldNames) {
        this.logFingerFieldNames = logFingerFieldNames;
    }

    public SQLLogFingerprintFilter getLogFingerprintFilter() {
        return logFingerprintFilter;
    }

    public void setLogFingerprintFilter(SQLLogFingerprintFilter logFingerprintFilter) {
        this.logFingerprintFilter = logFingerprintFilter;
    }

    public String getLogFingerFilterStageName() {
        return logFingerFilterStageName;
    }

    public void setLogFingerFilterStageName(String logFingerFilterStageName) {
        this.logFingerFilterStageName = logFingerFilterStageName;
    }
    public String getFilterFieldNames() {
        return filterFieldNames;
    }

    public void setFilterFieldNames(String filterFieldNames) {
        this.filterFieldNames = filterFieldNames;
    }
}
