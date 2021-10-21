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
package org.apache.rocketmq.streams.window.operator.impl;

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.streams.common.context.MessageHeader;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.datatype.IJsonable;
import org.apache.rocketmq.streams.common.datatype.IntDataType;
import org.apache.rocketmq.streams.common.datatype.ListDataType;
import org.apache.rocketmq.streams.common.datatype.MapDataType;
import org.apache.rocketmq.streams.common.datatype.StringDataType;
import org.apache.rocketmq.streams.common.utils.DataTypeUtil;
import org.apache.rocketmq.streams.window.model.WindowCache;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.operator.AbstractWindow;
import org.apache.rocketmq.streams.window.shuffle.ShuffleChannel;

public class TopNState implements IJsonable {
    private static final String SIGN ="#@#%@" ;
    private static final String NULL ="<NULL>" ;
    protected List<String> sortValues=new ArrayList<>();
    protected Map<String,String> orderByValue2Msgs=new HashMap<>();
    protected int topN=100;
    protected boolean isChanged=false;
    protected transient ListDataType listDataType;
    protected transient MapDataType mapDataType;
    public TopNState(int topN){
        this();
        this.topN=topN;
    }
    public TopNState(){
        listDataType=new ListDataType();
        listDataType.setParadigmType(new StringDataType());
        mapDataType=new MapDataType();
        mapDataType.setKeyParadigmType(new StringDataType());
        mapDataType.setValueParadigmType(new StringDataType());
    }
    /**
     *
     * @return msg order by orderbyFields
     */
    public List<JSONObject> getOrderMsgs(String rowNumerName, Set<String> fieldNames){
        List<JSONObject> msgs=new ArrayList<>();
        for(int i=0;i<sortValues.size();i++){
            JSONObject jsonObject=JSONObject.parseObject(orderByValue2Msgs.get(sortValues.get(i)));
            JSONObject msg=new JSONObject(jsonObject);
            msg.remove(WindowCache.ORIGIN_QUEUE_ID);
            msg.remove(WindowCache.SHUFFLE_KEY);
            msg.remove(WindowCache.ORIGIN_OFFSET);
            msg.remove(WindowCache.ORIGIN_QUEUE_IS_LONG);
            msg.remove(WindowCache.ORIGIN_MESSAGE_HEADER);
            msg.remove(WindowCache.ORIGIN_MESSAGE_TRACE_ID);
            msg.remove(WindowCache.ORIGIN_SOURCE_NAME);
            msg.remove(WindowInstance.class.getSimpleName());
            msg.remove(AbstractWindow.class.getSimpleName());
            msg.remove(MessageHeader.class.getSimpleName());
            msg.remove("HIT_WINDOW_INSTANCE_ID");
            msg.remove(ShuffleChannel.SHUFFLE_OFFSET);
            msg.remove(AbstractWindow.WINDOW_START);
            msg.remove(AbstractWindow.WINDOW_END);
            msg.put(rowNumerName,i+1);
            msgs.add(msg);
        }
        return msgs;
    }

    /**
     * add msg and sort
     * @param message
     * @param orderByList
     * @return
     */
    public boolean addAndSortMsg(JSONObject message,List<OrderBy> orderByList){
        String orderByValue=createOrderByValue(message,orderByList);

        if(sortValues.size()<topN){
            this.orderByValue2Msgs.put(orderByValue,message.toJSONString());
            this.sortValues.add(orderByValue);
        }else {
            String lastValue=sortValues.get(sortValues.size()-1);
            if(compareElement(orderByValue,lastValue,orderByList)<0){
                sortValues.add(orderByValue);
                this.orderByValue2Msgs.put(orderByValue,message.toJSONString());
            }else {
                return false;
            }
        }
        isChanged=true;
        Collections.sort(sortValues, new Comparator<String>() {
            @Override public int compare(String o1, String o2) {
                return compareElement(o1,o2,orderByList);
            }
        });
        while (sortValues.size()>topN){
            String sortValue=sortValues.remove(sortValues.size()-1);
            this.orderByValue2Msgs.remove(sortValue);
        }
        return true;
    }

    private int compareElement(String left, String right,List<OrderBy> orderByList) {
        if(left.equals(right)){
            return 0;
        }
        String[] leftValues=left.split(SIGN);
        String[] rigthValues=right.split(SIGN);
        int len=leftValues.length>right.length()?right.length():left.length();
        for(int i=0;i<len;i++){
            String leftElement=leftValues[i];
            String rigthElement=rigthValues[i];
            if(leftElement.equals(rigthElement)){
                continue;
            }
            OrderBy orderBy=orderByList.get(i);
            DataType dataType=orderBy.getDataType();
            boolean isAsc=orderBy.isAsc;
            if(DataTypeUtil.isNumber(dataType)){
                Double leftDouble=Double.valueOf(leftElement);
                Double rigthDoubel=Double.valueOf(rigthElement);
                if(isAsc){
                    return leftDouble-rigthDoubel<0?-1:1;
                }else {
                    return rigthDoubel-leftDouble>0?1:-1;
                }
            }else {
                if(isAsc){
                    return leftElement.compareTo(rigthElement);
                }else {
                    return rigthElement.compareTo(leftElement);
                }
            }
        }
        return 0;

    }

    public static void main(String[] args) {
        List<OrderBy> orders=new ArrayList<>();
        OrderBy orderBy=new OrderBy("name",false);
        orderBy.setDataType(new StringDataType());
        orders.add(orderBy);
        orderBy=new OrderBy("age",false);
        orderBy.setDataType(new IntDataType());
        orders.add(orderBy);
        TopNState topNState=new TopNState(3);
        JSONObject msg1=new JSONObject();
        msg1.put("name","chris2");
        msg1.put("age",18);
        topNState.addAndSortMsg(msg1,orders);
        JSONObject msg2=new JSONObject();
        msg2.put("name","chris2");
        msg2.put("age",19);
        topNState.addAndSortMsg(msg2,orders);
        JSONObject msg3=new JSONObject();
        msg3.put("name","chris1");
        msg3.put("age",18);
        topNState.addAndSortMsg(msg3,orders);

        JSONObject msg4=new JSONObject();
        msg4.put("name","chris1");
        msg4.put("age",19);
        topNState.addAndSortMsg(msg4,orders);
        System.out.println(topNState.sortValues.size());
    }


    protected String createOrderByValue(JSONObject message, List<OrderBy> list) {
        StringBuilder stringBuilder=new StringBuilder();
        boolean isFirst=true;
        for(OrderBy orderBy:list){
            Object  object=message.get(orderBy.getFieldName());
            DataType dataType=orderBy.getDataType();
            if(dataType==null&&object!=null){
                dataType= DataTypeUtil.getDataTypeFromClass(object.getClass());
                orderBy.setDataType(dataType);
            }
            String value="<null>";
            if(object!=null){
                value=dataType.toDataJson(object);
            }
            if(isFirst){
                stringBuilder.append(value);
                isFirst=false;
            }else {
                stringBuilder.append(SIGN);
                stringBuilder.append(value);
            }
        }
        return stringBuilder.toString();
    }

    public List<String> getSortValues() {
        return sortValues;
    }

    public void setSortValues(List<String> sortValues) {
        this.sortValues = sortValues;
    }

    public int getTopN() {
        return topN;
    }

    public void setTopN(int topN) {
        this.topN = topN;
    }

    public Map<String, String> getOrderByValue2Msgs() {
        return orderByValue2Msgs;
    }

    public void setOrderByValue2Msgs(Map<String, String> orderByValue2Msgs) {
        this.orderByValue2Msgs = orderByValue2Msgs;
    }

    public boolean isChanged() {
        return isChanged;
    }

    public void setChanged(boolean changed) {
        isChanged = changed;
    }

    @Override public String toJson() {
        JSONObject jsonObject=new JSONObject();
        jsonObject.put("sortValues",listDataType.toDataJson(this.sortValues));
        jsonObject.put("topN",this.topN);
        jsonObject.put("changed",this.isChanged?1:0);
        jsonObject.put("orderByValue2Msgs",mapDataType.toDataJson(this.orderByValue2Msgs));
        return jsonObject.toJSONString();
    }

    @Override public void toObject(String jsonString) {
        JSONObject jsonObject=JSONObject.parseObject(jsonString);
        this.topN=jsonObject.getInteger("topN");
        this.isChanged=jsonObject.getInteger("changed")==1?true:false;
        this.sortValues=listDataType.getData(jsonObject.getString("sortValues"));
        this.orderByValue2Msgs=mapDataType.getData(jsonObject.getString("orderByValue2Msgs"));
    }
}
