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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.Context;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.context.MessageHeader;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.dim.model.AbstractDim;
import org.apache.rocketmq.streams.window.model.WindowCache;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.operator.AbstractShuffleWindow;
import org.apache.rocketmq.streams.window.state.WindowBaseValue;
import org.apache.rocketmq.streams.window.state.impl.JoinLeftState;
import org.apache.rocketmq.streams.window.state.impl.JoinRightState;
import org.apache.rocketmq.streams.window.state.impl.JoinState;

public class JoinWindow extends AbstractShuffleWindow {

    public static final String JOIN_KEY = "JOIN_KEY";

    public static final String LABEL_LEFT = "left";

    public static final String LABEL_RIGHT = "right";

    //保存多少个周期的数据。比如window的滚动周期是5分钟，join需要1个小时数据，则retainWindowCount=12
    protected int retainWindowCount = 4;
    protected List<String> leftJoinFieldNames;//join等值条件中，左流的字段列表
    protected List<String> rightJoinFieldNames;//join等值条件中，右流的字段列表
    protected String rightAsName;//主要用于sql场景，默认右流都需要有别名。开发模式不需要

    protected String joinType;//join类型，值为INNER,LEFT
    protected String expression;//条件表达式。在存在非等值比较时使用
    protected transient DBOperator joinOperator = new DBOperator();

    @Override
    protected boolean initConfigurable() {
        //        return super.initConfigurable();
        super.initConfigurable();
        doProcessAfterRefreshConfigurable(null);
        return true;
    }

    //    @Override
    //    protected void addPropertyToMessage(IMessage oriMessage, JSONObject oriJson){
    //        oriJson.put("AbstractWindow", this);
    //
    //    }

    @Override
    protected int fireWindowInstance(WindowInstance instance, String shuffleId, Map<String, String> queueId2Offsets) {
        clearFire(instance);
        return 0;
    }

    @Override
    public void clearCache(String queueId) {

    }

    @Override
    public void shuffleCalculate(List<IMessage> messages, WindowInstance instance, String queueId) {
        Map<String, WindowBaseValue> joinLeftStates = new HashMap<>();
        Map<String, WindowBaseValue> joinRightStates = new HashMap<>();
        for (IMessage msg : messages) {
            MessageHeader header = JSONObject.parseObject(msg.getMessageBody().
                getString(WindowCache.ORIGIN_MESSAGE_HEADER), MessageHeader.class);
            msg.setHeader(header);
            String routeLabel = header.getMsgRouteFromLable();
            String storeKey = createStoreKey(msg, routeLabel, instance);
            JoinState state = createJoinState(msg, instance, routeLabel);
            if ("left".equalsIgnoreCase(routeLabel)) {
                joinLeftStates.put(storeKey, state);
            } else if ("right".equalsIgnoreCase(routeLabel)) {
                joinRightStates.put(storeKey, state);
            }
        }

        if (joinLeftStates.size() > 0) {
            storage.multiPut(joinLeftStates);
        }
        if (joinRightStates.size() > 0) {
            storage.multiPut(joinRightStates);
        }

        for (IMessage msg : messages) {
            MessageHeader header = JSONObject.parseObject(msg.getMessageBody().
                getString(WindowCache.ORIGIN_MESSAGE_HEADER), MessageHeader.class);
            String routeLabel = header.getMsgRouteFromLable();
            //            Map<String,WindowBaseValue> joinMessages = new HashMap<>();
            String storeKeyPrefix = "";
            Iterator<WindowBaseValue> iterator = null;
            if (LABEL_LEFT.equalsIgnoreCase(routeLabel)) {
                storeKeyPrefix = createStoreKeyPrefix(msg, LABEL_RIGHT, instance);
                iterator = getMessageIterator(queueId, instance, msg, storeKeyPrefix, JoinRightState.class);
            } else if (LABEL_RIGHT.equalsIgnoreCase(routeLabel)) {
                storeKeyPrefix = createStoreKeyPrefix(msg, LABEL_LEFT, instance);
                iterator = getMessageIterator(queueId, instance, msg, storeKeyPrefix, JoinLeftState.class);
            }

            //            Iterator<WindowBaseValue> iterator = getMessageIterator(queueId, instance, msg, storeKeyPrefix, JoinState.class);
            List<WindowBaseValue> tmpMessages = new ArrayList<>();
            int count = 0;
            while (iterator.hasNext()) {
                tmpMessages.add(iterator.next());
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

    private Iterator<WindowBaseValue> getMessageIterator(String queueId, WindowInstance instance,
                                                         IMessage msg, String keyPrefix, Class<? extends WindowBaseValue> clazz) {

        List<WindowInstance> instances = new ArrayList<>();
        for (Map.Entry<String, WindowInstance> entry : this.windowInstanceMap.entrySet()) {
            instances.add(entry.getValue());
        }
        Iterator<WindowInstance> windowInstanceIter = instances.iterator();
        return new Iterator<WindowBaseValue>() {
            protected volatile boolean hasNext = true;
            protected AtomicBoolean hasInit = new AtomicBoolean(false);
            protected Iterator<WindowBaseValue> iterator = null;

            @Override
            public boolean hasNext() {
                if (iterator != null && iterator.hasNext()) {
                    return true;
                }
                while (windowInstanceIter.hasNext()) {
                    WindowInstance instance = windowInstanceIter.next();
                    iterator = storage.loadWindowInstanceSplitData(null, queueId,
                        instance.createWindowInstanceId(),
                        keyPrefix,
                        clazz);
                    if (iterator != null && iterator.hasNext()) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public WindowBaseValue next() {
                return iterator.next();
            }
        };

    }

    private Iterator<WindowBaseValue> getIterator(String queueId, String keyPrefix, WindowInstance instance, Class<? extends WindowBaseValue> clazz) {

        List<WindowInstance> instances = new ArrayList<>();
        for (Map.Entry<String, WindowInstance> entry : this.windowInstanceMap.entrySet()) {
            instances.add(entry.getValue());
        }
        Iterator<WindowInstance> windowInstanceIter = instances.iterator();
        return new Iterator<WindowBaseValue>() {
            protected volatile boolean hasNext = true;
            protected AtomicBoolean hasInit = new AtomicBoolean(false);
            protected Iterator<WindowBaseValue> iterator = null;

            @Override
            public boolean hasNext() {
                if (iterator != null && iterator.hasNext()) {
                    return true;
                }
                if (windowInstanceIter.hasNext()) {
                    WindowInstance instance = windowInstanceIter.next();
                    iterator = storage.loadWindowInstanceSplitData(null, queueId, instance.createWindowInstanceId(), keyPrefix, clazz);
                    if (iterator != null && iterator.hasNext()) {
                        return true;
                    } else {
                        return false;
                    }
                }
                return false;
            }

            @Override
            public WindowBaseValue next() {
                return iterator.next();
            }
        };

    }

    public List<JSONObject> connectJoin(IMessage message, List<Map<String, Object>> rows, String joinType, String rightAsName) {
        List<JSONObject> result = new ArrayList<>();
        if (rows.size() <= 0) {
            return result;
        }
        if ("inner".equalsIgnoreCase(joinType)) {
            result = connectInnerJoin(message, rows, rightAsName);
        }
        //        else if ("left".equalsIgnoreCase(joinType)) {
        //            result = connectLeftJoin(message, rows, rightAsName);
        //        }
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
        if (LABEL_LEFT.equalsIgnoreCase(routeLabel)) {
            JSONObject messageBody = message.getMessageBody();
            for (Map<String, Object> raw : rows) {
                //                addAsName(raw, rightAsName);
                JSONObject object = (JSONObject)messageBody.clone();
                object.fluentPutAll(addAsName(raw, rightAsName));
                result.add(object);
            }
        } else {
            JSONObject messageBody = message.getMessageBody();
            messageBody = addAsName(messageBody, rightAsName);
            for (Map<String, Object> raw : rows) {
                JSONObject object = (JSONObject)messageBody.clone();
                object.fluentPutAll(raw);
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
            //            raw.remove(tmp.getKey());
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
        String shuffleId = shuffleChannel.getChannelQueue(shuffleKey).getQueueId();
        String orginQueueId = message.getMessageBody().getString(WindowCache.ORIGIN_QUEUE_ID);
        String originOffset = message.getMessageBody().getString(WindowCache.ORIGIN_OFFSET);
        String windowNamespace = getNameSpace();
        String windowName = getConfigureName();
        String startTime = windowInstance.getStartTime();
        String endTime = windowInstance.getEndTime();
        String storeKey = MapKeyUtil.createKey(shuffleId, windowNamespace, windowName, startTime, endTime, shuffleKey, routeLabel, orginQueueId, originOffset);
        return storeKey;
    }

    protected String createStoreKeyPrefix(IMessage message, String routeLabel, WindowInstance windowInstance) {
        String shuffleKey = message.getMessageBody().getString(WindowCache.SHUFFLE_KEY);
        String storeKey = MapKeyUtil.createKey(shuffleKey, routeLabel);
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
        JSONObject messageBody = (JSONObject)message.getMessageBody().clone();
        messageBody.remove("WindowInstance");
        messageBody.remove("AbstractWindow");

        JoinState state = null;
        if ("left".equalsIgnoreCase(routeLabel)) {
            state = new JoinLeftState();
        } else if ("right".equalsIgnoreCase(routeLabel)) {
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
        state.setWindowInstanceId(instance.getWindowInstanceKey());
        state.setPartitionNum(incrementAndGetSplitNumber(instance, shuffleId));
        state.setWindowInstancePartitionId(instance.getWindowInstanceKey());

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
    public static String generateKey(JSONObject messageBody, String joinLabel, List<String> leftJoinFieldNames, List<String> rightJoinFieldNames) {
        StringBuffer buffer = new StringBuffer();
        if ("left".equalsIgnoreCase(joinLabel)) {
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

        return StringUtil.createMD5Str(buffer.toString());
    }

    @Override
    protected String generateShuffleKey(IMessage message) {
        String routeLabel = message.getHeader().getMsgRouteFromLable();
        String messageKey = generateKey(message.getMessageBody(), routeLabel, leftJoinFieldNames, rightJoinFieldNames);
        return messageKey;
    }

    @Override
    public Class getWindowBaseValueClass() {
        return JoinState.class;
    }

    //    @Override
    //    public void finishWindowProcessAndSend2Receiver(List<IMessage> messageList,WindowInstance windowInstance) {
    //        for (IMessage message : messageList) {
    //            List<Map<String, Object>> result = joinOperator.dealJoin(message);
    //            List<Map<String,Object>> rows = matchRows(message.getMessageBody(), result);
    //            String rightAsName = message.getMessageBody().getString("rightAsName");
    //            String joinType = message.getMessageBody().getString("joinType");
    //            List<JSONObject> connectMsgs = joinOperator.connectJoin(message, rows, joinType, rightAsName);
    //            for (int i=0; i < connectMsgs.size(); i++) {
    //                if (i == connectMsgs.size() -1) {
    //                    sendMessage(connectMsgs.get(i), true);
    //                } else {
    //                    sendMessage(connectMsgs.get(i), false);
    //                }
    //
    //            }
    //
    //        }
    //        //todo 完成处理
    //        //todo 发送消息到下一个节点 sendFireMessage();
    //    }

    /**
     * window触发后的清理工作
     * @param windowInstances
     */
    /**
     * 删除掉触发过的数据
     *
     * @param instance
     */
    @Override
    public void clearFireWindowInstance(WindowInstance instance) {
        if(instance==null){
            return;
        }
        WindowInstance.clearInstance(instance);
        joinOperator.cleanMessage(instance.getWindowNameSpace(), instance.getWindowName(), this.getRetainWindowCount(),
            this.getSizeInterval(), instance.getStartTime());
        //todo windowinstace
        //todo left+right
    }

    protected List<Map<String, Object>> matchRows(JSONObject msg, List<Map<String, Object>> rows) {

        return AbstractDim.matchExpressionByLoop(rows.iterator(), expression, msg, true);
    }

    private List<Map<String, Object>> converToMapFromList(List<WindowBaseValue> rows) {
        List<Map<String, Object>> joinMessages = new ArrayList<>();
        for (WindowBaseValue value : rows) {
            JSONObject obj = Message.parseObject(((JoinState)value).getMessageBody());
            joinMessages.add((Map<String, Object>)obj);
        }
        return joinMessages;
    }

    /**
     * 把触发的数据，发送到下一个节点
     *
     * @param message
     * @param needFlush
     */
    protected void sendMessage(JSONObject message, boolean needFlush) {
        Message nextMessage = new Message(message);
        if (needFlush) {
            nextMessage.getHeader().setNeedFlush(true);
        }
        AbstractContext context = new Context(nextMessage);
        this.getFireReceiver().doMessage(nextMessage, context);
    }

    protected void sendMessage(IMessage msg, List<WindowBaseValue> messages) {
        if (messages == null || messages.size() == 0) {
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

    @Override protected Long queryWindowInstanceMaxSplitNum(WindowInstance instance) {
        Long leftMaxSplitNum=storage.getMaxSplitNum(instance,JoinLeftState.class);
        Long rigthMaxSplitNum=storage.getMaxSplitNum(instance,JoinRightState.class);
        if(leftMaxSplitNum==null){
            return rigthMaxSplitNum;
        }
        if(rigthMaxSplitNum==null){
            return leftMaxSplitNum;
        }
        if(leftMaxSplitNum>=rigthMaxSplitNum){
            return leftMaxSplitNum;
        }
        if(leftMaxSplitNum<rigthMaxSplitNum){
            return rigthMaxSplitNum;
        }
        return null;
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
