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

package org.apache.rocketmq.streams.filter.optimization;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.streams.common.calssscaner.AbstractScan;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.filter.operator.expression.SimpleExpression;
import org.apache.rocketmq.streams.script.optimization.performance.AbstractScriptProxy;
import org.apache.rocketmq.streams.script.service.IScriptExpression;

public class ExpressionProxyFactory {
    protected List<AbstractExpressionProxy> expressionProxies = new ArrayList<>();
    protected static ExpressionProxyFactory expressionProxyFactory=new ExpressionProxyFactory();
    protected static AtomicBoolean isFinishScan = new AtomicBoolean(false);
    protected AbstractScan scan = new AbstractScan() {
        @Override protected void doProcessor(Class clazz) {
            if (AbstractExpressionProxy.class.isAssignableFrom(clazz)&& !Modifier.isAbstract(clazz.getModifiers())) {
                AbstractExpressionProxy abstractExpressionProxy = (AbstractExpressionProxy) ReflectUtil.forInstance(clazz, new Class[] {Expression.class, Rule.class}, new Object[] {null,null});
                expressionProxies.add(abstractExpressionProxy);
            }
        }
    };

    public static ExpressionProxyFactory getInstance() {
        if (isFinishScan.compareAndSet(false, true)) {
            expressionProxyFactory.scan.scanPackages("org.apache.rocketmq.streams.script.optimization");
            expressionProxyFactory.scan.scanPackages("org.apache.rocketmq.streams.filter.optimization");
        }
        return expressionProxyFactory;
    }

    public AbstractExpressionProxy create(Expression expression,Rule rule) {
        for (AbstractExpressionProxy abstractExpressionProxy : expressionProxies) {
            if (abstractExpressionProxy.support(expression)) {
                return (AbstractExpressionProxy) ReflectUtil.forInstance(abstractExpressionProxy.getClass(), new Class[] {Expression.class, Rule.class}, new Object[] {expression,rule});
            }
        }
        return null;
    }
}