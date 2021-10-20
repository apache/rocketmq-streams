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
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.datatype.IntDataType;
import org.apache.rocketmq.streams.common.datatype.StringDataType;
import org.apache.rocketmq.streams.common.utils.DataTypeUtil;

public class TopNState extends BasedConfigurable {
    private static final String SIGN ="#@#%@" ;
    private static final String NULL ="<NULL>" ;
    protected List<String> sortValues=new ArrayList<>();
    protected Map<String,String> orderByValue2Msgs=new HashMap<>();
    protected int topN=100;
    protected boolean isChanged=false;
    public TopNState(int topN){
        this.topN=topN;
    }
    public TopNState(){
    }
    /**
     *
     * @return msg order by orderbyFields
     */
    public List<JSONObject> getOrderMsgs(String rowNumerName){
        List<JSONObject> msgs=new ArrayList<>();
        for(int i=0;i<sortValues.size();i++){
            JSONObject msg=JSONObject.parseObject(orderByValue2Msgs.get(sortValues.get(i)));
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
}
