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
package org.apache.rocketmq.streams.common.context;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.cache.compress.BitSetCache;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.monitor.IMonitor;
import org.apache.rocketmq.streams.common.monitor.MonitorFactory;
import org.apache.rocketmq.streams.common.optimization.FilterResultCache;
import org.apache.rocketmq.streams.common.optimization.HomologousVar;
import org.apache.rocketmq.streams.common.topology.metric.NotFireReason;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;

public abstract class AbstractContext<T extends IMessage> extends HashMap {

    public static final String MESSAGE_KEY = "message_context";//如果context需要存在message中，可以用这个key

    protected static final String CLASS_NAME = "className";
    /**
     * cache filter（regex，like，equals）result
     */
    private static String FILTER_CACHE_PREPIX = "__filter_cache_prefix";
    @Deprecated
    protected Map<String, Object> values = new HashMap<String, Object>();
    /**
     * 是否退出循环，只有在循环模式中应用 在移步stage中，使用，如果消息是因为在stage中被过滤了，则设置这个值为true
     */
    protected boolean isBreak = false;
    protected boolean isContinue = true;
    protected T message;
    /**
     * 必须强制设置才回生效
     */
    protected boolean isSplitModel = false;
    /**
     * 如果消息在执行过程中做了拆分，把拆分后的消息放入这个字段
     */
    protected List<T> splitMessages = new ArrayList<T>();
    protected volatile IMonitor monitor = null;
    protected FilterResultCache quickFilterResult;
    protected Map<String, BitSetCache.BitSet> homologousResult;
    //未触发规则的表达式
    protected List<String> notFireExpressionMonitor = new ArrayList<>();

    public AbstractContext(T message) {
        this.message = message;
    }

    public boolean isContinue() {
        return isContinue;
    }

    public T breakExecute() {
        isContinue = false;
        return message;
    }

    public void cancelBreak() {
        isContinue = true;
    }

    public <C extends AbstractContext<T>> void syncContext(C subContext) {
        this.putAll(subContext);
        this.setValues(subContext.getValues());
        this.setSplitModel(subContext.isSplitModel());
        this.setMessage(subContext.getMessage());
        this.setSplitMessages(subContext.getSplitMessages());
        this.monitor = subContext.monitor;
        this.isBreak = subContext.isBreak;
        this.quickFilterResult = subContext.quickFilterResult;
        this.homologousResult = subContext.homologousResult;
        this.isContinue = subContext.isContinue;
        this.notFireExpressionMonitor = subContext.notFireExpressionMonitor;
    }

    public <C extends AbstractContext<T>> C syncSubContext(C subContext) {
        subContext.putAll(this);
        subContext.setValues(this.getValues());
        subContext.setSplitModel(this.isSplitModel());
        subContext.setMessage(this.getMessage());
        subContext.setSplitMessages(this.getSplitMessages());
        subContext.monitor = this.monitor;
        subContext.isBreak = isBreak;
        subContext.quickFilterResult = quickFilterResult;
        subContext.homologousResult = homologousResult;
        subContext.isContinue = isContinue;
        subContext.notFireExpressionMonitor = notFireExpressionMonitor;
        return subContext;
    }

    /**
     * match from cache , if not exist cache return null
     *
     * @param expression
     * @return
     */
    public Boolean matchFromCache(IMessage message, Object expression) {
        if (quickFilterResult != null) {
            return quickFilterResult.isMatch(message, expression);
        }
        return null;
    }

    public Boolean matchFromHomologousCache(IMessage message, HomologousVar var) {
        if (var == null) {
            return null;
        }
        if (this.homologousResult == null) {
            return null;
        }

        BitSetCache.BitSet bitSet = this.homologousResult.get(var.getHomologousVarKey());
        if (bitSet == null) {
            return null;
        }
        return bitSet.get(var.getIndex());
    }

    public void resetIsContinue() {
        isContinue = true;
    }

    public T getMessage() {
        return message;
    }

    public void setMessage(T message) {
        this.message = message;
    }

    public List<T> getSplitMessages() {
        return splitMessages;
    }

    public void setSplitMessages(List<T> splitMessages) {
        this.splitMessages = splitMessages;
    }

    public void addSplitMessages(List<T> splitMessages) {
        this.splitMessages.remove(message);
        this.splitMessages.addAll(splitMessages);
    }

    public void addSplitMessages(T... splitMessages) {
        this.splitMessages.remove(message);
        if (splitMessages == null) {
            return;
        }
        this.splitMessages.addAll(Arrays.asList(splitMessages));
    }

    public void removeSpliteMessage(T message) {
        this.splitMessages.remove(message);
    }

    public boolean isSplitModel() {
        return isSplitModel;
    }

    public void setSplitModel(boolean splitModel) {
        isSplitModel = splitModel;
    }

    public void openSplitModel() {
        isSplitModel = true;
    }

    public void setFilterCache(String expressionStr, String varValue, boolean result) {
        this.put(MapKeyUtil.createKey(FILTER_CACHE_PREPIX, expressionStr, varValue), result);
    }

    /**
     * 获取基于字段缓存的某些值
     *
     * @param fieldName
     * @param <T>
     * @return
     */
    @Deprecated
    public <T> T getValue(String fieldName) {
        return (T) values.get(fieldName);
    }

    /**
     * 暂存数据，不会改变message的值
     *
     * @param key
     * @param value
     */
    @Deprecated
    public void putValue(String key, Object value) {
        values.put(key, value);
    }

    @Deprecated
    public void removeValue(String key) {
        values.remove(key);
    }

    public void closeSplitMode(T message) {
        this.setSplitModel(false);
        this.splitMessages = new ArrayList<>();
        this.message = message;
    }

    public Map<String, Object> getValues() {
        return values;
    }

    public void setValues(Map<String, Object> values) {
        this.values = values;
    }

    /**
     * 启动monitor，如果monitor已经启动，则重复调用可重入
     *
     * @param name
     * @return
     */
    public IMonitor startMonitor(String name) {
        if (monitor != null) {
            return monitor;
        }
        monitor = MonitorFactory.createMonitor(name);
        return monitor;
    }

    public IMonitor getCurrentMonitorItem(String... parentNames) {
        if (monitor != null) {
            if (monitor.getChildren() == null || monitor.getChildren().size() == 0) {
                return monitor;
            } else {
                return monitor.getChildren().get(monitor.getChildren().size() - 1);
            }
        } else {
            monitor = startMonitor(MapKeyUtil.createKeyBySign(".", parentNames));
            return monitor;
        }
    }

    public abstract AbstractContext<T> copy();

    protected void copyProperty(AbstractContext context) {
        Map<String, Object> values = new HashMap<>();
        values.putAll(this.getValues());
        context.setValues(values);
        context.putAll(this);
        context.setSplitModel(this.isSplitModel());
        List<T> messages = new ArrayList<>();
        for (T tmp : this.getSplitMessages()) {
            messages.add(tmp.deepCopy());
        }
        context.setSplitMessages(messages);
        context.monitor = this.monitor;
        context.homologousResult = homologousResult;
    }

    public IMonitor getMonitor() {
        return monitor;
    }

    public IMonitor createChildrenMonitor(String parentMintorName, IConfigurable configurable) {
        IMonitor monitor = getMonitor();
        if (monitor == null) {
            monitor = startMonitor(parentMintorName);

        }
        return monitor.createChildren(configurable);
    }

    public NotFireReason getNotFireReason() {
        return (NotFireReason) this.get("NotFireReason");
    }

    public void setNotFireReason(NotFireReason notFireReason) {
        this.put("NotFireReason", notFireReason);
    }

    public void removeNotFireReason() {
        this.remove("NotFireReason");
    }

    public boolean isBreak() {
        return isBreak;
    }

    public void setBreak(boolean aBreak) {
        isBreak = aBreak;
    }

    public Map<String, BitSetCache.BitSet> getHomologousResult() {
        return homologousResult;
    }

    public void setHomologousResult(
        Map<String, BitSetCache.BitSet> homologousResult) {
        this.homologousResult = homologousResult;
    }

    public List<String> getNotFireExpressionMonitor() {
        return notFireExpressionMonitor;
    }

    public void setNotFireExpressionMonitor(List<String> notFireExpressionMonitor) {
        this.notFireExpressionMonitor = notFireExpressionMonitor;
    }

    public FilterResultCache getQuickFilterResult() {
        return quickFilterResult;
    }

    public void setQuickFilterResult(FilterResultCache quickFilterResult) {
        this.quickFilterResult = quickFilterResult;
    }

}
