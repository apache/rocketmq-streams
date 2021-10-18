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
package org.apache.rocketmq.streams.filter.optimization.dependency;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.stages.ScriptChainStage;
import org.apache.rocketmq.streams.script.operator.impl.FunctionScript;
import org.apache.rocketmq.streams.script.service.IScriptExpression;

public class ScriptTreeNode extends TreeNode<ScriptChainStage> {
    protected transient Map<String,IScriptExpression> varName2Scripts=new HashMap<>();
    protected transient Map<String, List<String>> varName2DependentFields=new HashMap<>();


    public ScriptTreeNode(ChainPipeline pipeline, ScriptChainStage stage,
        TreeNode parent) {
        super(pipeline, stage, parent);
        FunctionScript functionScript=(FunctionScript)stage.getScript();
        for(IScriptExpression expression:functionScript.getScriptExpressions()){
            if(expression.getNewFieldNames()!=null&&expression.getNewFieldNames().size()==1){
                String newFieldName=expression.getNewFieldNames().iterator().next();
                varName2Scripts.put(newFieldName,expression);
                varName2DependentFields.put(newFieldName,expression.getDependentFields());
            }
        }
    }

    /**
     * trace root field dependent in this script
     * @param varName
     * @return
     */
    @Override
    public Set<String> traceaField(String varName) {
        Set<String> fields=new HashSet<>();
        List<String> depenentFields=varName2DependentFields.get(varName);
        if(depenentFields==null){
            fields.add(varName);
            return fields;
        }
        for(String newFieldName:depenentFields){
            Set<String> dependentFields=traceaField(newFieldName);
            fields.addAll(dependentFields);
        }
        return fields;
    }

    public List<IScriptExpression> getDependencyExpression(String varName) {
        IScriptExpression scriptExpression= this.varName2Scripts.get(varName);
        if(scriptExpression==null){
            return new ArrayList<>();
        }
        List<IScriptExpression> list=new ArrayList<>();
        list.add(scriptExpression);
        List<String> fields=scriptExpression.getDependentFields();
        if(fields!=null){
            for(String fieldName:fields){
                List<IScriptExpression> dependents=getDependencyExpression(fieldName);
                if(dependents==null){
                    return null;
                }else {
                    list.addAll(dependents);
                }
            }
        }
        Collections.sort(list, new Comparator<IScriptExpression>() {
            @Override public int compare(IScriptExpression o1, IScriptExpression o2) {
                List<String> varNames1= o1.getDependentFields();
                List<String> varNames2=o2.getDependentFields();
                for(String varName:varNames1){
                    if(o2.getNewFieldNames()!=null&&o2.getNewFieldNames().contains(varName)){
                        return 1;
                    }
                }
                for(String varName:varNames2){
                    if(o1.getNewFieldNames()!=null&&o1.getNewFieldNames().contains(varName)){
                        return -1;
                    }
                }
                return 0;
            }
        });
        return list;
    }

}
