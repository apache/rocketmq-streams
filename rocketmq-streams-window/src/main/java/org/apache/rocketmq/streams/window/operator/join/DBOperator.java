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

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.context.MessageHeader;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.db.driver.orm.ORMUtil;
import org.apache.rocketmq.streams.window.state.impl.JoinLeftState;
import org.apache.rocketmq.streams.window.state.impl.JoinRightState;
import org.apache.rocketmq.streams.window.state.impl.JoinState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBOperator implements Operator {

    private static final Logger LOGGER = LoggerFactory.getLogger(DBOperator.class);

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

        return MD5(buffer.toString());
    }

    public static String MD5(String s) {
        char hexDigits[] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

        try {
            byte[] btInput = s.getBytes();
            // 获得MD5摘要算法的 MessageDigest 对象
            MessageDigest mdInst = MessageDigest.getInstance("MD5");
            // 使用指定的字节更新摘要
            mdInst.update(btInput);
            // 获得密文
            byte[] md = mdInst.digest();
            // 把密文转换成十六进制的字符串形式
            int j = md.length;
            char str[] = new char[j * 2];
            int k = 0;
            for (int i = 0; i < j; i++) {
                byte byte0 = md[i];
                str[k++] = hexDigits[byte0 >>> 4 & 0xf];
                str[k++] = hexDigits[byte0 & 0xf];
            }
            return new String(str);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /**
     * 根据join流对原始数据进行处理并入库
     *
     * @param messageList
     * @param joinType
     */
    public void addMessage(List<IMessage> messageList, String joinType) {
        List<JoinLeftState> joinLeftStates = new ArrayList<JoinLeftState>();
        List<JoinRightState> joinRightStates = new ArrayList<JoinRightState>();
        for (IMessage message : messageList) {
            String routeLabel = message.getHeader().getMsgRouteFromLable();
            JoinState state = dealMessge(message, routeLabel);
            if ("left".equalsIgnoreCase(routeLabel)) {
                joinLeftStates.add((JoinLeftState) state);
            } else if ("right".equalsIgnoreCase(routeLabel)) {
                joinRightStates.add((JoinRightState) state);
            }
            //            joinStates.add(state);
        }

        if (joinLeftStates.size() > 0) {
            ORMUtil.batchReplaceInto(joinLeftStates);
        }
        if (joinRightStates.size() > 0) {
            ORMUtil.batchReplaceInto(joinRightStates);
        }

    }

    /**
     * 生成joinstate对象
     *
     * @param message
     * @param routeLabel
     * @return
     */
    public JoinState dealMessge(IMessage message, String routeLabel) {

        JSONObject window = message.getMessageBody().getJSONObject("Window");
        String windowName = window.getString("configureName");
        String windowNameSpace = window.getString("nameSpace");
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

        String messageId = windowNameSpace + "_" + windowName + "_" + queueId + "_" + offset;

        List<String> leftJoinFieldNames = window.getJSONArray("leftJoinFieldNames") != null ?
            toJavaList(window.getJSONArray("leftJoinFieldNames")) :
            new ArrayList<>();
        List<String> rightJoinFieldNames = window.getJSONArray("rightJoinFieldNames") != null ?
            toJavaList(window.getJSONArray("rightJoinFieldNames")) :
            new ArrayList<>();

        String messageKey = generateKey(message.getMessageBody(), routeLabel, leftJoinFieldNames, rightJoinFieldNames);
        JSONObject messageBody = (JSONObject) message.getMessageBody().clone();
        messageBody.remove("WindowInstance");
        messageBody.remove("Window");

        JoinState state = null;
        if ("left".equalsIgnoreCase(routeLabel)) {
            state = new JoinLeftState();
        } else if ("right".equalsIgnoreCase(routeLabel)) {
            state = new JoinRightState();
        }

        state.setGmtCreate(new Date());
        state.setGmtModified(new Date());
        state.setWindowName(windowName);
        state.setWindowNameSpace(windowNameSpace);
        state.setMessageId(messageId);
        state.setMessageKey(messageKey);
        state.setMessageTime(new Date());
        state.setMessageBody(messageBody.toJSONString());

        return state;
    }

    private List<String> toJavaList(JSONArray jsonArray) {
        List<String> list = new ArrayList<String>(jsonArray.size());

        for (Object item : jsonArray) {
            if (item == null) {
                list.add(null);
            } else if (String.class.isInstance(item)) {
                list.add((String) item);
            } else {
                list.add(item.toString());
            }

        }

        return list;
    }

    public List<Map<String, Object>> dealJoin(IMessage message) {
        List<Map<String, Object>> result = new ArrayList<>();
        JSONObject messageBody = message.getMessageBody();
        JSONObject msg = messageBody.getJSONObject("msg");
        String routeLabel = messageBody.getString("routeLabel");
        JSONArray windowInstances = msg.getJSONArray("WindowInstance");
        JSONObject windowInstance = null;
        if (windowInstances != null && windowInstances.size() > 0) {
            windowInstance = windowInstances.getJSONObject(0);
        } else {
            return result;
        }

        Integer retainWindowCount = messageBody.getInteger("retainWindowCount");
        Integer sizeInterval = messageBody.getInteger("sizeInterval");

        List<JSONObject> tmp = new ArrayList<>();
        if ("left".equalsIgnoreCase(routeLabel)) {
            String endTime = windowInstance.getString("endTime");
            String startTime = addTime(windowInstance.getString("startTime"), TimeUnit.MINUTES, -retainWindowCount * sizeInterval);
            String tableName = "join_right_state";
            String messageKey = messageBody.getString("messageKey");
            String windowName = windowInstance.getString("windowName");
            String windowNameSpace = windowInstance.getString("windowNameSpace");
            tmp = getJoinData(tableName, messageKey, windowName, windowNameSpace, startTime, endTime);

        } else if ("right".equalsIgnoreCase(routeLabel)) {
            //            String startTime = addTime(windowInstance.getString("startTime"), TimeUnit.MINUTES, -sizeInterval);
            String startTime = addTime(windowInstance.getString("startTime"), TimeUnit.MINUTES, -retainWindowCount * sizeInterval);
            String endTime = addTime(windowInstance.getString("endTime"), TimeUnit.MINUTES, -sizeInterval);
            String tableName = "join_left_state";
            String messageKey = messageBody.getString("messageKey");
            String windowName = windowInstance.getString("windowName");
            String windowNameSpace = windowInstance.getString("windowNameSpace");
            tmp = getJoinData(tableName, messageKey, windowName, windowNameSpace, startTime, endTime);
        }

        result = converToMapFromJson(tmp);
        return result;

    }

    public List<JSONObject> connectJoin(IMessage message, List<Map<String, Object>> rows, String joinType,
        String rightAsName) {
        List<JSONObject> result = new ArrayList<>();
        if (rows.size() <= 0) {
            return result;
        }
        if ("inner".equalsIgnoreCase(joinType)) {
            result = connectInnerJoin(message, rows, rightAsName);
        } else if ("left".equalsIgnoreCase(joinType)) {
            result = connectLeftJoin(message, rows, rightAsName);
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
        String routeLabel = message.getMessageBody().getString("routeLabel");
        if ("left".equalsIgnoreCase(routeLabel)) {
            JSONObject messageBody = message.getMessageBody().getJSONObject("msg");
            for (Map<String, Object> raw : rows) {
                //                addAsName(raw, rightAsName);
                JSONObject object = (JSONObject) messageBody.clone();
                object.fluentPutAll(addAsName(raw, rightAsName));
                result.add(object);
            }
        } else {
            JSONObject messageBody = message.getMessageBody().getJSONObject("msg");
            messageBody = (JSONObject) addAsName(messageBody, rightAsName);
            for (Map<String, Object> raw : rows) {
                JSONObject object = (JSONObject) messageBody.clone();
                object.fluentPutAll(raw);
                result.add(object);
            }
        }

        return result;
    }

    private Map<String, Object> addAsName(Map<String, Object> raw, String rightAsName) {
        Map<String, Object> asName = new HashMap<>();
        for (Map.Entry<String, Object> tmp : raw.entrySet()) {
            asName.put(rightAsName + "." + tmp.getKey(), tmp.getValue());
            //            raw.remove(tmp.getKey());
        }
        return asName;
    }

    public List<JSONObject> connectLeftJoin(IMessage message, List<Map<String, Object>> rows, String rightAsName) {
        List<JSONObject> result = new ArrayList<>();
        String routeLabel = message.getMessageBody().getString("routeLabel");
        JSONObject messageBody = message.getMessageBody().getJSONObject("msg");
        if ("left".equalsIgnoreCase(routeLabel)) {
            if (rows != null && rows.size() > 0) {
                for (Map<String, Object> raw : rows) {
                    //                    raw = addAsName(raw, rightAsName);
                    JSONObject object = (JSONObject) messageBody.clone();
                    object.fluentPutAll(addAsName(raw, rightAsName));
                    result.add(object);
                }
                return result;
            } else {
                result.add(messageBody);
            }

        } else {
            if (rows != null && rows.size() > 0) {
                messageBody = (JSONObject) addAsName(messageBody, rightAsName);
                for (Map<String, Object> raw : rows) {
                    JSONObject object = (JSONObject) messageBody.clone();
                    object.fluentPutAll(raw);
                    result.add(object);
                }
                return result;
            }
        }

        return result;

    }

    private List<Map<String, Object>> converToMapFromJson(List<JSONObject> list) {
        List<Map<String, Object>> mapList = new ArrayList<>();
        if (list != null && list.size() > 0) {
            for (JSONObject object : list) {
                Map<String, Object> tmp = object;
                mapList.add(tmp);
            }
        }
        return mapList;
    }

    public List<JSONObject> getJoinData(String tableName, String messageKey, String windowName, String windowNameSpace,
        String startTime, String endTime) {
        Map<String, Object> paras = new HashMap<>();
        paras.put("messageKey", messageKey);
        paras.put("startTime", startTime);
        paras.put("endTime", endTime);
        paras.put("windowName", windowName);
        paras.put("windowNameSpace", windowNameSpace);
        //        paras.put("tableName", tableName);
        List<JoinState> result = new ArrayList<>();
        if ("join_right_state".equalsIgnoreCase(tableName)) {
            result = ORMUtil.queryForList("select message_body from join_right_state where message_key = #{messageKey} and window_name = #{windowName}" +
                "and window_name_space = #{windowNameSpace} and gmt_create >= #{startTime} and gmt_create < #{endTime}", paras, JoinState.class);
        } else if ("join_left_state".equalsIgnoreCase(tableName)) {
            result = ORMUtil.queryForList("select message_body from join_left_state where message_key = #{messageKey} and window_name = #{windowName} " +
                "and window_name_space = #{windowNameSpace} and gmt_create >= #{startTime} and gmt_create < #{endTime}", paras, JoinState.class);
        }

        List<JSONObject> bodys = new ArrayList<>();
        for (JoinState tmp : result) {
            try {
                bodys.add(Message.parseObject(tmp.getMessageBody()));
            } catch (Exception e) {
                LOGGER.error("json parase error:", e);
            }

        }
        return bodys;
    }

    public String addTime(String time, TimeUnit unit, int value) {
        Date date = DateUtil.parseTime(time);
        date = DateUtil.addDate(unit, date, value);
        return DateUtil.format(date);
    }

    /**
     * 根据window去除过期消息数据,消息去除时间为starttime加上窗口
     *
     * @param windowNameSpace
     * @param windowName
     * @param retainWindowCount
     * @param sizeInterval
     * @param startTime
     */
    public void cleanMessage(String windowNameSpace, String windowName, int retainWindowCount, int sizeInterval,
        String startTime) {
        Map<String, Object> params = new HashMap<>();
        String start = addTime(startTime, TimeUnit.MINUTES, -retainWindowCount * sizeInterval);
        params.put("startTime", start);
        params.put("windowNameSpace", windowNameSpace);
        params.put("windowName", windowName);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("dboperata delete param is " + JSONObject.toJSONString(params));
        }

        List<JoinLeftState> joinLeftStates = ORMUtil.queryForList("select id from join_left_state where window_name_space = #{windowNameSpace} and " +
            "window_name = #{windowName} and gmt_create < #{startTime}", params, JoinLeftState.class);
        if (joinLeftStates != null && joinLeftStates.size() > 0) {
            List<String> deleteIds = this.getDeleteIds(joinLeftStates);
            for (String ids : deleteIds) {
                //                params.put("ids", ids);
                ORMUtil.executeSQL("delete from join_left_state where id in (" + ids + ")", null);
            }

        }

        List<JoinRightState> joinRightStates = ORMUtil.queryForList("select id from join_right_state where window_name_space = #{windowNameSpace} and " +
            "window_name = #{windowName} and gmt_create < #{startTime}", params, JoinRightState.class);
        if (joinRightStates != null && joinRightStates.size() > 0) {
            List<String> deleteIds = this.getDeleteIds(joinRightStates);
            for (String ids : deleteIds) {
                //                params.put("ids", ids);
                ORMUtil.executeSQL("delete from join_right_state where id in (" + ids + ")", null);
            }

        }

    }

    private List<String> getDeleteIds(List<? extends JoinState> instances) {
        List<String> deteleIds = new ArrayList<>();
        if (instances == null || instances.size() == 0) {
            return deteleIds;
        }
        int count = 1;

        StringBuilder builder = new StringBuilder();
        for (; count <= instances.size(); count++) {
            builder.append(instances.get(count - 1).getId());
            if (count % 1000 == 0) {
                deteleIds.add(builder.toString());
                builder = new StringBuilder();
            } else {
                if (count == instances.size()) {
                    deteleIds.add(builder.toString());
                } else {
                    builder.append(",");
                }

            }
            //            count++;
        }

        return deteleIds;
    }

}