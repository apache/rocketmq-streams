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
package org.apache.rocketmq.streams.window.operator.join;

import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.Context;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.context.MessageHeader;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.common.utils.TraceUtil;
import org.apache.rocketmq.streams.dim.model.AbstractDim;
import org.apache.rocketmq.streams.window.model.WindowCache;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.operator.AbstractShuffleWindow;
import org.apache.rocketmq.streams.window.state.WindowBaseValue;
import org.apache.rocketmq.streams.window.state.impl.JoinLeftState;
import org.apache.rocketmq.streams.window.state.impl.JoinRightState;
import org.apache.rocketmq.streams.window.state.impl.JoinState;
import org.apache.rocketmq.streams.window.storage.IteratorWrap;
import org.apache.rocketmq.streams.window.storage.RocksdbIterator;
import org.apache.rocketmq.streams.window.storage.WindowJoinType;
import org.apache.rocketmq.streams.window.storage.WindowType;

import static org.apache.rocketmq.streams.window.shuffle.ShuffleChannel.SHUFFLE_OFFSET;

public class JoinWindow extends AbstractShuffleWindow {
    //保存多少个周期的数据。比如window的滚动周期是5分钟，join需要1个小时数据，则retainWindowCount=12
    protected int retainWindowCount = 4;
    protected List<String> leftJoinFieldNames;//join等值条件中，左流的字段列表
    protected List<String> rightJoinFieldNames;//join等值条件中，右流的字段列表
    protected String rightAsName;//主要用于sql场景，默认右流都需要有别名。开发模式不需要

    protected String joinType;//join类型，值为INNER,LEFT
    protected String expression;//条件表达式。在存在非等值比较时使用

    @Override
    protected int doFireWindowInstance(WindowInstance instance) {
        //todo 只是清理吗？
        clearFire(instance);
        return 0;
    }

    @Override
    public void clearCache(String queueId) {
        storage.clearCache(queueId);
    }

    @Override
    public void shuffleCalculate(List<IMessage> messages, WindowInstance instance, String queueId) {
        String windowInstanceId = instance.getWindowInstanceId();

        for (IMessage msg : messages) {
            MessageHeader header = JSONObject.parseObject(msg.getMessageBody().getString(WindowCache.ORIGIN_MESSAGE_HEADER), MessageHeader.class);
            msg.setHeader(header);
            String routeLabel = header.getMsgRouteFromLable();

            JoinState state = createJoinState(msg, instance, routeLabel);
            List<WindowBaseValue> temp = new ArrayList<>();
            temp.add(state);

            if (WindowJoinType.left.name().equalsIgnoreCase(routeLabel)) {
                storage.putWindowBaseValue(queueId, windowInstanceId, WindowType.JOIN_WINDOW, WindowJoinType.left, temp);

            } else if (WindowJoinType.right.name().equalsIgnoreCase(routeLabel)) {
                storage.putWindowBaseValue(queueId, windowInstanceId, WindowType.JOIN_WINDOW, WindowJoinType.right, temp);
            } else {
                throw new RuntimeException("param routeLabel: [" + routeLabel + "] error.");
            }

            Iterator<WindowBaseValue> iterator;
            if (WindowJoinType.left.name().equalsIgnoreCase(routeLabel)) {
                iterator = getMessageIterator(queueId, WindowJoinType.right);
            } else if (WindowJoinType.right.name().equalsIgnoreCase(routeLabel)) {
                iterator = getMessageIterator(queueId, WindowJoinType.left);
            } else {
                throw new RuntimeException("param routeLabel: [" + routeLabel + "] error.");
            }

            List<WindowBaseValue> tmpMessages = new ArrayList<>();
            int count = 0;
            while (iterator.hasNext()) {
                WindowBaseValue windowBaseValue = iterator.next();
                if (windowBaseValue == null) {
                    continue;
                }
                tmpMessages.add(windowBaseValue);
                count++;
                if (count == 100) {
                    sendMessage(msg, tmpMessages);
                    tmpMessages.clear();
                    count = 0;
                }
            }
            sendMessage(msg, tmpMessages);
        }

    }


    private Iterator<WindowBaseValue> getMessageIterator(String queueId, WindowJoinType joinType) {

        List<WindowInstance> instances = new ArrayList<>();
        for (Map.Entry<String, WindowInstance> entry : this.windowInstanceMap.entrySet()) {
            if (queueId.equalsIgnoreCase(entry.getValue().getSplitId())) {
                instances.add(entry.getValue());
            }
        }
        Iterator<WindowInstance> windowInstanceIter = instances.iterator();
        return new Iterator<WindowBaseValue>() {
            private RocksdbIterator<WindowBaseValue> iterator = null;

            @Override
            public boolean hasNext() {
                if (iterator != null && iterator.hasNext()) {
                    return true;
                }
                if (windowInstanceIter.hasNext()) {
                    WindowInstance instance = windowInstanceIter.next();
                    iterator = storage.getWindowBaseValue(instance.getSplitId(), instance.getWindowInstanceId(), WindowType.JOIN_WINDOW, joinType);
                    if (iterator != null && iterator.hasNext()) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public WindowBaseValue next() {
                return iterator.next().getData();
            }
        };

    }


    public List<JSONObject> connectJoin(IMessage message, List<Map<String, Object>> rows, String joinType,
                                        String rightAsName) {
        List<JSONObject> result = new ArrayList<>();

        if ("inner".equalsIgnoreCase(joinType)) {
            if (rows.size() <= 0) {
                return result;
            }
            result = connectInnerJoin(message, rows, rightAsName);
        } else if ("left".equalsIgnoreCase(joinType)) {
            result = connectLeftJoin(message, rows, rightAsName);
        }
        return result;
    }

    private List<JSONObject> connectLeftJoin(IMessage message, List<Map<String, Object>> rows, String rightAsName) {

        List<JSONObject> result = new ArrayList<>();
        String routeLabel = message.getHeader().getMsgRouteFromLable();
        JSONObject messageBody = message.getMessageBody();
        String traceId = message.getHeader().getTraceId();
        int index = 1;
        if (WindowJoinType.left.name().equalsIgnoreCase(routeLabel) && rows.size() > 0) {
            for (Map<String, Object> raw : rows) {
                JSONObject object = (JSONObject) messageBody.clone();
                object.fluentPutAll(addAsName(raw, rightAsName));
                object.put(TraceUtil.TRACE_ID_FLAG, traceId + "-" + index);
                index++;
                result.add(object);
            }
        } else if (WindowJoinType.left.name().equalsIgnoreCase(routeLabel) && rows.size() <= 0) {
            JSONObject object = (JSONObject) messageBody.clone();
            object.put(TraceUtil.TRACE_ID_FLAG, traceId + "-" + index);
            result.add(object);
        } else if (WindowJoinType.right.name().equalsIgnoreCase(routeLabel) && rows.size() > 0) {
            messageBody = addAsName(messageBody, rightAsName);
            for (Map<String, Object> raw : rows) {
                JSONObject object = (JSONObject) messageBody.clone();
                object.fluentPutAll(raw);
                object.put(TraceUtil.TRACE_ID_FLAG, traceId + "-" + index);
                index++;
                result.add(object);
            }
        }
        return result;
    }

    /**
     * inner join 将匹配messageKey的各行与message进行连接
     *
     * @param message
     * @param rows
     * @return
     */
    public List<JSONObject> connectInnerJoin(IMessage message, List<Map<String, Object>> rows, String rightAsName) {
        List<JSONObject> result = new ArrayList<>();
        String routeLabel = message.getHeader().getMsgRouteFromLable();
        String traceId = message.getHeader().getTraceId();
        int index = 1;
        if (WindowJoinType.left.name().equalsIgnoreCase(routeLabel)) {
            JSONObject messageBody = message.getMessageBody();
            for (Map<String, Object> raw : rows) {
                JSONObject object = (JSONObject) messageBody.clone();
                object.fluentPutAll(addAsName(raw, rightAsName));
                object.put(TraceUtil.TRACE_ID_FLAG, traceId + "-" + index);
                index++;
                result.add(object);
            }
        } else {
            JSONObject messageBody = message.getMessageBody();
            messageBody = addAsName(messageBody, rightAsName);
            for (Map<String, Object> raw : rows) {
                JSONObject object = (JSONObject) messageBody.clone();
                object.fluentPutAll(raw);
                object.put(TraceUtil.TRACE_ID_FLAG, traceId + "-" + index);
                index++;
                result.add(object);
            }
        }

        return result;
    }

    private JSONObject addAsName(Map<String, Object> raw, String rightAsName) {
        JSONObject object = new JSONObject();
        if (StringUtil.isEmpty(rightAsName)) {
            return object.fluentPutAll(raw);
        }
        for (Map.Entry<String, Object> tmp : raw.entrySet()) {
            object.put(rightAsName + "." + tmp.getKey(), tmp.getValue());
        }
        return object;
    }

    /**
     * 生成join消息key值 全局唯一 key值构成结构为 shuffleId：shuffe split id, windowNamespace：窗口命名空间, windowName： 窗口名称, startTime：窗口开始时间, endTime：窗口结束时间, shuffleKey： join key值, routeLabel：消息左右流标记, orginQueueId：原始消息queueid, originOffset：原始消息offset
     *
     * @param message
     * @param routeLabel
     * @param windowInstance
     * @return
     */
    protected String createStoreKey(IMessage message, String routeLabel, WindowInstance windowInstance) {
        String shuffleKey = message.getMessageBody().getString(WindowCache.SHUFFLE_KEY);
        String orginQueueId = message.getMessageBody().getString(WindowCache.ORIGIN_QUEUE_ID);
        String originOffset = message.getMessageBody().getString(WindowCache.ORIGIN_OFFSET);
        String storeKey = MapKeyUtil.createKey(windowInstance.getWindowInstanceId(), shuffleKey, routeLabel, orginQueueId, originOffset);
        return storeKey;
    }


    /**
     * 根据左右流标志对原始消息进行封装
     *
     * @param message    原始消息
     * @param instance
     * @param routeLabel 左右流标志
     * @return
     */
    private JoinState createJoinState(IMessage message, WindowInstance instance, String routeLabel) {
        MessageHeader header = message.getHeader();
        String queueId = "_Dipper";
        String offset = System.nanoTime() + "";
        if (header.getQueueId() != null) {
            queueId = header.getQueueId();
        }
        if (header.isEmptyOffset()) {
            header.setOffset(offset);
            offset = header.getOffset();
        }

        String messageId = this.getNameSpace() + "_" + this.getConfigureName() + "_" + queueId + "_" + offset;

        String messageKey = generateKey(message.getMessageBody(), routeLabel, this.leftJoinFieldNames, this.rightJoinFieldNames);
        JSONObject messageBody = (JSONObject) message.getMessageBody().clone();
        messageBody.remove("WindowInstance");
        messageBody.remove("AbstractWindow");
        messageBody.remove(WindowCache.ORIGIN_MESSAGE_HEADER);
        messageBody.remove("MessageHeader");

        JoinState state = null;
        if (WindowJoinType.left.name().equalsIgnoreCase(routeLabel)) {
            state = new JoinLeftState();
        } else if (WindowJoinType.right.name().equalsIgnoreCase(routeLabel)) {
            state = new JoinRightState();
        }

        state.setGmtCreate(new Date());
        state.setGmtModified(new Date());
        state.setWindowName(this.getConfigureName());
        state.setWindowNameSpace(this.getNameSpace());
        state.setMessageId(messageId);
        state.setMessageKey(messageKey);
        state.setMessageTime(new Date());
        state.setMessageBody(messageBody.toJSONString());
        state.setMsgKey(createStoreKey(message, routeLabel, instance));
        String shuffleKey = message.getMessageBody().getString(WindowCache.SHUFFLE_KEY);
        String shuffleId = shuffleChannel.getChannelQueue(shuffleKey).getQueueId();
        state.setPartition(shuffleId);
        state.setWindowInstanceId(instance.getWindowInstanceId());
        state.setPartitionNum(incrementAndGetSplitNumber(instance, shuffleId));

        return state;
    }

    /**
     * 根据join条件生成消息比对key值
     *
     * @param messageBody
     * @param joinLabel
     * @param leftJoinFieldNames
     * @param rightJoinFieldNames
     * @return
     */
    public static String generateKey(JSONObject messageBody, String joinLabel, List<String> leftJoinFieldNames,
                                     List<String> rightJoinFieldNames) {
        StringBuffer buffer = new StringBuffer();
        if (WindowJoinType.left.name().equalsIgnoreCase(joinLabel)) {
            for (String field : leftJoinFieldNames) {
                String value = messageBody.getString(field);
                buffer.append(value).append("_");
            }
        } else {
            for (String field : rightJoinFieldNames) {
                String[] rightFields = field.split("\\.");
                if (rightFields.length > 1) {
                    field = rightFields[1];
                }
                String value = messageBody.getString(field);
                buffer.append(value).append("_");
            }
        }

        return buffer.toString();
    }

    @Override
    public String generateShuffleKey(IMessage message) {
        String routeLabel = message.getHeader().getMsgRouteFromLable();
        String messageKey = generateKey(message.getMessageBody(), routeLabel, leftJoinFieldNames, rightJoinFieldNames);
        return messageKey;
    }


    @Override
    public synchronized void clearFireWindowInstance(WindowInstance windowInstance) {
        List<WindowInstance> removeInstances = new ArrayList<>();

        Date clearTime = DateUtil.addSecond(DateUtil.parse(windowInstance.getStartTime()), -sizeInterval * (retainWindowCount - 1) * 60);
        Iterator<String> iterable = this.windowInstanceMap.keySet().iterator();
        while (iterable.hasNext()) {
            WindowInstance instance = this.windowInstanceMap.get(iterable.next());
            Date startTime = DateUtil.parse(instance.getStartTime());
            if (DateUtil.dateDiff(clearTime, startTime) >= 0) {
                removeInstances.add(instance);
                iterable.remove();
            }
        }

        for (WindowInstance instance : removeInstances) {
            //清理MaxPartitionNum
            storage.deleteMaxPartitionNum(instance.getSplitId(), instance.getWindowInstanceId());

            //从windowInstance表中删除
            storage.deleteWindowInstance(instance.getSplitId(), this.getNameSpace(), this.getConfigureName(), instance.getWindowInstanceId());


            //从JoinState表中删除
            deleteFromJoinState(instance, WindowJoinType.right);
            deleteFromJoinState(instance, WindowJoinType.left);
        }
    }

    private void deleteFromJoinState(WindowInstance instance, WindowJoinType windowJoinType) {

        RocksdbIterator<JoinState> joinStates = storage.getWindowBaseValue(instance.getSplitId(), instance.getWindowInstanceId(), WindowType.JOIN_WINDOW, windowJoinType);
        while (joinStates.hasNext()) {
            IteratorWrap<JoinState> next = joinStates.next();

            JoinState joinState = next.getData();
            Date start = addTime(instance.getStartTime(), TimeUnit.MINUTES, -retainWindowCount * sizeInterval);

            if (canDelete(instance, joinState, start)) {
                storage.deleteWindowBaseValue(instance.getSplitId(), instance.getWindowInstanceId(), WindowType.JOIN_WINDOW, windowJoinType);
            }
        }
    }

    private boolean canDelete(WindowInstance instance, JoinState joinState, Date start) {
        return instance.getWindowNameSpace().equals(joinState.getWindowNameSpace())
                && instance.getWindowName().equals(joinState.getWindowName())
                && instance.getGmtCreate().getTime() < start.getTime();
    }

    private Date addTime(String time, TimeUnit unit, int value) {
        Date date = DateUtil.parseTime(time);
        return DateUtil.addDate(unit, date, value);
    }

    protected List<Map<String, Object>> matchRows(JSONObject msg, List<Map<String, Object>> rows) {

        return AbstractDim.matchExpressionByLoop(rows.iterator(), expression, msg, true);
    }

    private List<Map<String, Object>> converToMapFromList(List<WindowBaseValue> rows) {
        List<Map<String, Object>> joinMessages = new ArrayList<>();
        for (WindowBaseValue value : rows) {
            JSONObject obj = Message.parseObject(((JoinState) value).getMessageBody());
            joinMessages.add((Map<String, Object>) obj);
        }
        return joinMessages;
    }

    protected transient AtomicInteger count = new AtomicInteger(0);

    /**
     * 把触发的数据，发送到下一个节点
     *
     * @param message
     * @param needFlush
     */
    protected void sendMessage(JSONObject message, boolean needFlush) {
        Message nextMessage = new Message(message);
        cleanMessage(nextMessage);
        if (needFlush) {
            nextMessage.getHeader().setNeedFlush(true);
        }
        AbstractContext context = new Context(nextMessage);
        boolean isWindowTest = ComponentCreator.getPropertyBooleanValue("window.fire.isTest");
        if (isWindowTest) {
            System.out.println(getConfigureName() + " result send count is " + count.incrementAndGet());
        }
        this.getFireReceiver().doMessage(nextMessage, context);
    }

    protected void sendMessage(IMessage msg, List<WindowBaseValue> messages) {
        if ("inner".equalsIgnoreCase(joinType) && (messages == null || messages.size() == 0)) {
            return;
        }
        List<JSONObject> connectMsgs;
        if (this.expression == null) {
            List<Map<String, Object>> rows = converToMapFromList(messages);
            connectMsgs = connectJoin(msg, rows, joinType, rightAsName);
        } else {
            List<Map<String, Object>> rows = matchRows(msg.getMessageBody(), converToMapFromList(messages));
            connectMsgs = connectJoin(msg, rows, joinType, rightAsName);
        }
        for (int i = 0; i < connectMsgs.size(); i++) {
            if (i == connectMsgs.size() - 1) {
                sendMessage(connectMsgs.get(i), true);
            } else {
                sendMessage(connectMsgs.get(i), false);
            }
        }
    }

    protected void cleanMessage(Message msg) {
        JSONObject messageBody = msg.getMessageBody();
        messageBody.remove("WindowInstance");
        messageBody.remove("AbstractWindow");
        messageBody.remove(WindowCache.ORIGIN_MESSAGE_HEADER);
        messageBody.remove("MessageHeader");
        messageBody.remove(SHUFFLE_OFFSET);
        messageBody.remove("HIT_WINDOW_INSTANCE_ID");
        messageBody.remove(TraceUtil.TRACE_ID_FLAG);
        messageBody.remove(WindowCache.ORIGIN_QUEUE_ID);
        messageBody.remove(WindowCache.SHUFFLE_KEY);
        messageBody.remove(WindowCache.ORIGIN_MESSAGE_TRACE_ID);
        messageBody.remove(WindowCache.ORIGIN_OFFSET);
        messageBody.remove(WindowCache.ORIGIN_QUEUE_IS_LONG);
    }

    @Override
    public void removeInstanceFromMap(WindowInstance windowInstance) {
        String begin = DateUtil.getBeforeMinutesTime(windowInstance.getStartTime(), (this.retainWindowCount - 1) * this.sizeInterval);
        String deletePrefix = MapKeyUtil.createKey(windowInstance.getWindowNameSpace(), windowInstance.getWindowName(), begin);
        for (Map.Entry<String, WindowInstance> tmp : windowInstanceMap.entrySet()) {
            if (tmp.getKey().compareToIgnoreCase(deletePrefix) <= 0) {
                windowInstanceMap.remove(tmp);
            }
        }
    }


    @Override
    public boolean supportBatchMsgFinish() {
        return false;
    }

    public int getRetainWindowCount() {
        return retainWindowCount;
    }

    public void setRetainWindowCount(int retainWindowCount) {
        this.retainWindowCount = retainWindowCount;
    }

    public List<String> getLeftJoinFieldNames() {
        return leftJoinFieldNames;
    }

    public void setLeftJoinFieldNames(List<String> leftJoinFieldNames) {
        this.leftJoinFieldNames = leftJoinFieldNames;
    }

    public List<String> getRightJoinFieldNames() {
        return rightJoinFieldNames;
    }

    public void setRightJoinFieldNames(List<String> rightJoinFieldNames) {
        this.rightJoinFieldNames = rightJoinFieldNames;
    }

    public String getRightAsName() {
        return rightAsName;
    }

    public void setRightAsName(String rightAsName) {
        this.rightAsName = rightAsName;
    }

    public String getJoinType() {
        return joinType;
    }

    public void setJoinType(String joinType) {
        this.joinType = joinType;
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }
}
