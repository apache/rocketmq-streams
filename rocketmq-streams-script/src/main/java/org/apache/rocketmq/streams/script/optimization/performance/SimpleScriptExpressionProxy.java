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

package org.apache.rocketmq.streams.script.optimization.performance;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.optimization.cachefilter.AbstractCacheFilter;
import org.apache.rocketmq.streams.common.optimization.cachefilter.ICacheFilter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.operator.expression.ScriptParameter;
import org.apache.rocketmq.streams.script.service.IScriptExpression;
import org.apache.rocketmq.streams.script.service.IScriptParamter;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

public abstract class SimpleScriptExpressionProxy extends AbstractScriptProxy {

    public SimpleScriptExpressionProxy(IScriptExpression origExpression) {
        super(origExpression);
    }
    protected List<ICacheFilter> optimizationExpressions=null;
    @Override
    public List<ICacheFilter> getCacheFilters() {
        IScriptExpression scriptExpression=this.origExpression;
        if(this.optimizationExpressions==null){
            synchronized (this){
                if(this.optimizationExpressions==null){
                    List<ICacheFilter> optimizationExpressions=new ArrayList<>();
                    optimizationExpressions.add(new AbstractCacheFilter<IScriptExpression>(getVarName(),this.origExpression) {
                        @Override public boolean executeOrigExpression(IMessage message, AbstractContext context) {
                            FunctionContext functionContext = new FunctionContext(message);
                            if (context != null) {
                                context.syncSubContext(functionContext);
                            }
                            Boolean isMatch=(Boolean)scriptExpression.executeExpression(message,functionContext);

                            if (context != null) {
                                context.syncContext(functionContext);
                            }
                            return isMatch;
                        }

                        @Override public String getExpression() {
                            return getParameterValue((IScriptParamter)origExpression.getScriptParamters().get(1));
                        }
                    });
                    this.optimizationExpressions=optimizationExpressions;
                }
            }
        }
        return this.optimizationExpressions;

    }



    @Override public Object executeExpression(IMessage message, FunctionContext context) {
        Boolean value= this.optimizationExpressions.get(0).execute(message,context);
        if(this.origExpression.getNewFieldNames()!=null&&this.origExpression.getNewFieldNames().size()>0){
            message.getMessageBody().put(this.origExpression.getNewFieldNames().iterator().next(), value);
        }
        return value;
    }


    protected abstract String getVarName();
}
