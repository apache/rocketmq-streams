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
package org.apache.rocketmq.streams.filter.optimization.casewhen;

import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.script.context.FunctionContext;

public class MutilCaseWhenExpression extends AbstractWhenExpression {
    public MutilCaseWhenExpression(String namespace, String name) {
        super(namespace, name);
    }

    @Override public Object executeExpression(IMessage message, FunctionContext context) {
        for (String key : varNames2GroupByVarCaseWhen.keySet()) {
            executeGroupByVarCaseWhen(key, varNames2GroupByVarCaseWhen.get(key), message, context);
        }
        if (notCacheCaseWhenElement != null) {
            for (CaseWhenElement caseWhenElement : this.notCacheCaseWhenElement) {
                caseWhenElement.executeDirectly(message, context);
            }
        }
        return null;
    }

    @Override protected boolean executeThenDirectly() {
        return true;
    }
}
