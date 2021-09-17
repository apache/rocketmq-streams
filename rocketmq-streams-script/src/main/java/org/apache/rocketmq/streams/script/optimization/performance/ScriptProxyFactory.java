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

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.streams.common.calssscaner.AbstractScan;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.script.service.IScriptExpression;

public class ScriptProxyFactory {
    protected List<AbstractScriptProxy> expressionProxies = new ArrayList<>();
    protected static ScriptProxyFactory expressionProxyFactory = new ScriptProxyFactory();
    protected static AtomicBoolean isFinishScan = new AtomicBoolean(false);
    protected AbstractScan scan = new AbstractScan() {
        @Override protected void doProcessor(Class clazz) {
            if (AbstractScriptProxy.class.isAssignableFrom(clazz) && !Modifier.isAbstract(clazz.getModifiers())) {
                AbstractScriptProxy abstractExpressionProxy = (AbstractScriptProxy) ReflectUtil.forInstance(clazz, new Class[] {IScriptExpression.class}, new Object[] {null});
                expressionProxies.add(abstractExpressionProxy);
            }
        }
    };

    public static ScriptProxyFactory getInstance() {
        if (isFinishScan.compareAndSet(false, true)) {
            expressionProxyFactory.scan.scanPackages("org.apache.rocketmq.streams.script.optimization.performance");
            expressionProxyFactory.scan.scanPackages("org.apache.rocketmq.streams.filter.optimization");
        }
        return expressionProxyFactory;
    }

    public AbstractScriptProxy create(IScriptExpression oriScriptExpression) {
        for (AbstractScriptProxy abstractExpressionProxy : expressionProxies) {
            abstractExpressionProxy.setOrigExpression(oriScriptExpression);
            if (abstractExpressionProxy.supportOptimization(oriScriptExpression)) {
                return (AbstractScriptProxy) ReflectUtil.forInstance(abstractExpressionProxy.getClass(), new Class[] {IScriptExpression.class}, new Object[] {oriScriptExpression});
            }
        }
        return null;
    }

}
