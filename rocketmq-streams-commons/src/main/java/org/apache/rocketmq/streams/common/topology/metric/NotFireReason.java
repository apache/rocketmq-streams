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
package org.apache.rocketmq.streams.common.topology.metric;

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.optimization.IHomologousOptimization;
import org.apache.rocketmq.streams.common.optimization.fingerprint.FingerprintCache;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.stages.FilterChainStage;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.common.utils.JsonableUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;

public class NotFireReason {
    protected List<String> oriFilterFieldNames=new ArrayList<>();//参与过滤的字段名
    protected  Map<String,String> oriFilterFields=new HashMap<>();//原始


    protected List<String> expressions=new ArrayList<>();//过滤失败的表达式
    protected Map<String,String> filterFields=new HashMap<>();//参与过滤的字段名和值
    protected Map<String, List<String>> filterFieldName2ETLScriptList=new HashMap<>();//过滤字段和ETL脚本
    private List<String> filterFieldNames=new ArrayList<>();//过滤字段列表

    protected transient ChainPipeline pipeline;
    protected transient FilterChainStage stage;
    public NotFireReason(FilterChainStage stage,String fieldValues){
        this.stage=stage;
        this.pipeline=(ChainPipeline) stage.getPipeline();
        String logFingerFieldNames=this.stage.getPreFingerprint().getLogFingerFieldNames();
        if(logFingerFieldNames==null){
            return;
        }
        String[] values=logFingerFieldNames.split(",");
        for(String oriFieldName:values){
            oriFilterFieldNames.add(oriFieldName);
        }
        values=fieldValues.split(FingerprintCache.FIELD_VALUE_SPLIT_SIGN);
        for(int i=0;i<values.length;i++){
            String fieldName=oriFilterFieldNames.get(i);
            String fiedlValue=values[i];
            oriFilterFields.put(fieldName,fiedlValue);
        }
    }
    public void analysis(IMessage message,Map<String, List<String>> filterFieldName2ETLScriptList,Map<String, String> filterFieldName2OriFieldName,List<String> expressions,List<String> filterFieldNames){
        if(oriFilterFields.size()==0){
            return;
        }
        this.expressions.addAll(expressions);
        Map<String, List<String>> etlScript=new HashMap<>();
        this.filterFieldNames.addAll(filterFieldNames);
        Map<String,String> oriFilterFields=new HashMap<>();
        List<String> oriFilterFieldNames=new ArrayList<>();
        for(String filteFieldName:filterFieldNames){
            String oriFieldName=filterFieldName2OriFieldName.get(filteFieldName);
            String oriFieldValue=this.oriFilterFields.get(oriFieldName);
            if(oriFieldName!=null&&oriFieldValue!=null){
                oriFilterFields.put(oriFieldName,oriFieldValue);
            }
            String filterValue=message.getMessageBody().getString(filteFieldName);
            if(filterValue!=null){
                filterFields.put(filteFieldName,filterValue);
            }

            if(oriFieldName!=null){
                oriFilterFieldNames.add(oriFieldName);
            }
            List<String> etl=filterFieldName2ETLScriptList.get(filteFieldName);
            if(etl!=null){
                etlScript.put(oriFieldName,etl);
            }

        }
        this.filterFieldName2ETLScriptList=etlScript;
        this.oriFilterFields=oriFilterFields;
        this.oriFilterFieldNames=oriFilterFieldNames;
    }

    public List<String> getOriFilterFieldNames() {
        return oriFilterFieldNames;
    }

    public void setOriFilterFieldNames(List<String> oriFilterFieldNames) {
        this.oriFilterFieldNames = oriFilterFieldNames;
    }

    public Map<String, String> getOriFilterFields() {
        return oriFilterFields;
    }

    public void setOriFilterFields(Map<String, String> oriFilterFields) {
        this.oriFilterFields = oriFilterFields;
    }

    public List<String> getExpressions() {
        return expressions;
    }

    public void setExpressions(List<String> expressions) {
        this.expressions = expressions;
    }

    public Map<String, String> getFilterFields() {
        return filterFields;
    }

    public void setFilterFields(Map<String, String> filterFields) {
        this.filterFields = filterFields;
    }

    public Map<String, List<String>> getFilterFieldName2ETLScriptList() {
        return filterFieldName2ETLScriptList;
    }

    public void setFilterFieldName2ETLScriptList(
        Map<String, List<String>> filterFieldName2ETLScriptList) {
        this.filterFieldName2ETLScriptList = filterFieldName2ETLScriptList;
    }

    public List<String> getFilterFieldNames() {
        return filterFieldNames;
    }

    public void setFilterFieldNames(List<String> filterFieldNames) {
        this.filterFieldNames = filterFieldNames;
    }

    public FilterChainStage getStage() {
        return stage;
    }

    public ChainPipeline getPipeline() {
        return pipeline;
    }
    @Override public String toString() {
        return JsonableUtil.formatJson(toJson());
    }
     public JSONObject toJson() {
        JSONObject jsonObject=new JSONObject();
        jsonObject.put("表达式和数据", MapKeyUtil.createKey("\n",this.expressions));

         JSONObject values=new JSONObject();


        if(CollectionUtil.isNotEmpty(this.filterFields)){
            values.putAll(this.filterFields);
        }


        if(CollectionUtil.isNotEmpty(this.oriFilterFields)){
            values.putAll(this.oriFilterFields);

        }

        if(CollectionUtil.isNotEmpty(this.filterFieldName2ETLScriptList)){
            List<String> strings=new ArrayList<>();
            for(List<String> scripts:this.filterFieldName2ETLScriptList.values()){
                strings.add(MapKeyUtil.createKey("<br>",scripts));
                strings.add("<p>");
            }
            jsonObject.put("etl","<br>"+MapKeyUtil.createKeyFromCollection("<br>",strings));
        }
         if(CollectionUtil.isNotEmpty(values)){
             jsonObject.put("field value",values);
         }

        return jsonObject;
    }
}
