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
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.interfaces.IBaseStreamOperator;
import org.apache.rocketmq.streams.common.model.ThreadContext;
import org.apache.rocketmq.streams.common.monitor.IMonitor;
import org.apache.rocketmq.streams.common.monitor.MonitorFactory;
import org.apache.rocketmq.streams.common.optimization.quicker.QuickFilterResult;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;

public abstract class AbstractContext<T extends IMessage> extends HashMap {

    public static final String MESSAGE_KEY = "message_context";//如果context需要存在message中，可以用这个key

    protected static final String CLASS_NAME = "className";

    protected transient IConfigurableService configurableService;

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


    protected QuickFilterResult quickFilterResult;

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
        this.setConfigurableService(subContext.getConfigurableService());
        this.setSplitModel(subContext.isSplitModel());
        this.setMessage(subContext.getMessage());
        this.setSplitMessages(subContext.getSplitMessages());
        this.monitor = subContext.monitor;
        this.isBreak = subContext.isBreak;
        this.quickFilterResult=subContext.quickFilterResult;
    }

    public <C extends AbstractContext<T>> C syncSubContext(C subContext) {
        subContext.putAll(this);
        subContext.setValues(this.getValues());
        subContext.setConfigurableService(this.getConfigurableService());
        subContext.setSplitModel(this.isSplitModel());
        subContext.setMessage(this.getMessage());
        subContext.setSplitMessages(this.getSplitMessages());
        subContext.monitor = this.monitor;
        subContext.isBreak = isBreak;
        subContext.quickFilterResult=quickFilterResult;

        return subContext;
    }

    /**
     * match from cache , if not exist cache return null
     * @param varName
     * @param expression
     * @return
     */
    public Boolean matchFromCache(String varName,String expression){
        if(quickFilterResult!=null){
            return quickFilterResult.isMatch(varName,expression);
        }
        return null;
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

    public void openSplitModel() {
        isSplitModel = true;
    }

    /**
     * cache filter（regex，like，equals）result
     */
    private static String FILTER_CACHE_PREPIX = "__filter_cache_prefix";

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

    public void setSplitModel(boolean splitModel) {
        isSplitModel = splitModel;
    }

    public void closeSplitMode(T message) {
        this.setSplitModel(false);
        this.splitMessages = new ArrayList<>();
        this.message = message;
    }

    public Map<String, Object> getValues() {
        return values;
    }

    public IConfigurableService getConfigurableService() {
        return configurableService;
    }

    public void setConfigurableService(IConfigurableService configurableService) {
        this.configurableService = configurableService;
    }

    public void setValues(Map<String, Object> values) {
        this.values = values;
    }

    public static <R, C extends AbstractContext> List<IMessage> executeScript(IMessage channelMessage, C context,
        List<? extends IBaseStreamOperator<IMessage, R, C>> scriptExpressions) {
        List<IMessage> messages = new ArrayList<>();
        if (scriptExpressions == null) {
            return messages;
        }
        boolean isSplitMode = context.isSplitModel();
        context.closeSplitMode(channelMessage);
        int nextIndex = 1;
        //long start=System.currentTimeMillis();
        executeScript(scriptExpressions.get(0), channelMessage, context, nextIndex, scriptExpressions);
        // System.out.println("================="+(System.currentTimeMillis()-start));
        if (!context.isContinue()) {
            context.setSplitModel(isSplitMode || context.isSplitModel());
            return null;
        }

        if (context.isSplitModel()) {
            messages = context.getSplitMessages();
        } else {
            messages.add(context.getMessage());
        }
        context.setSplitModel(isSplitMode || context.isSplitModel());
        return messages;
    }

    /**
     * 执行当前规则，如果规则符合拆分逻辑调拆分逻辑。为了是减少循环次数，一次循环多条规则
     *
     * @param currentExpression
     * @param channelMessage
     * @param context
     * @param nextIndex
     * @param scriptExpressions
     */
    private static <R, C extends AbstractContext> void executeScript(
        IBaseStreamOperator<IMessage, R, C> currentExpression,
        IMessage channelMessage, C context, int nextIndex,
        List<? extends IBaseStreamOperator<IMessage, R, C>> scriptExpressions) {
        //long start=System.currentTimeMillis();

        /**
         * 为了兼容blink udtf，通过localthread把context传给udtf的collector
         */
        ThreadContext threadContext = ThreadContext.getInstance();
        threadContext.set(context);
        currentExpression.doMessage(channelMessage, context);

        //System.out.println(currentExpression.toString()+" cost time is "+(System.currentTimeMillis()-start));
        if (context.isContinue() == false) {
            return;
        }
        if (nextIndex >= scriptExpressions.size()) {
            return;
        }
        IBaseStreamOperator<IMessage, R, C> nextScriptExpression = scriptExpressions.get(nextIndex);
        nextIndex++;
        if (context.isSplitModel()) {
            // start=System.currentTimeMillis();
            executeSplitScript(nextScriptExpression, channelMessage, context, nextIndex, scriptExpressions);

            //System.out.println(currentExpression.toString()+" cost time is "+(System.currentTimeMillis()-start));
        } else {
            executeScript(nextScriptExpression, channelMessage, context, nextIndex, scriptExpressions);
        }
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

    private static <R, C extends AbstractContext> void executeSplitScript(
        IBaseStreamOperator<IMessage, R, C> currentExpression, IMessage channelMessage, C context, int nextIndex,
        List<? extends IBaseStreamOperator<IMessage, R, C>> scriptExpressions) {
        if (context.getSplitMessages() == null || context.getSplitMessages().size() == 0) {
            return;
        }
        List<IMessage> result = new ArrayList<>();
        List<IMessage> splitMessages = new ArrayList<IMessage>();
        splitMessages.addAll(context.getSplitMessages());
        int splitMessageOffset = 0;
        for (IMessage message : splitMessages) {
            context.closeSplitMode(message);
            message.getHeader().addLayerOffset(splitMessageOffset);
            splitMessageOffset++;
            executeScript(currentExpression, message, context, nextIndex, scriptExpressions);
            if (!context.isContinue()) {
                context.cancelBreak();
                continue;
            }
            if (context.isSplitModel()) {
                result.addAll(context.getSplitMessages());
            } else {
                result.add(context.getMessage());
            }

        }
        context.openSplitModel();
        context.setSplitMessages(result);
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

    public abstract AbstractContext copy();

    protected void copyProperty(AbstractContext context) {
        Map<String, Object> values = new HashMap<>();
        values.putAll(this.getValues());
        context.setValues(values);
        context.putAll(this);
        context.setConfigurableService(this.getConfigurableService());
        context.setSplitModel(this.isSplitModel());
        List<T> messages = new ArrayList<>();
        for (T tmp : this.getSplitMessages()) {
            messages.add(tmp.deepCopy());
        }
        context.setSplitMessages(messages);
        context.monitor = this.monitor;
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

    public boolean isBreak() {
        return isBreak;
    }

    public void setBreak(boolean aBreak) {
        isBreak = aBreak;
    }

    public void setQuickFilterResult(QuickFilterResult quickFilterResult) {
        this.quickFilterResult = quickFilterResult;
    }
}
