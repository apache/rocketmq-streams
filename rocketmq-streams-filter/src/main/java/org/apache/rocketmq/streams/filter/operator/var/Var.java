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
package org.apache.rocketmq.streams.filter.operator.var;

import java.io.Serializable;

import org.apache.rocketmq.streams.filter.operator.action.IConfigurableAction;
import org.apache.rocketmq.streams.filter.context.RuleContext;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class Var<T> extends BasedConfigurable implements IConfigurable, IConfigurableAction<T>, Serializable {

    private static final Log LOG = LogFactory.getLog(Var.class);

    public static final String TYPE = "var";

    protected String varName;

    public Var() {
        setType(TYPE);
    }

    public String getVarName() {
        return varName;
    }

    public void setVarName(String varName) {
        this.varName = varName;
    }

    public Object getVarValue(RuleContext context, Rule rule) {
        if (context.containsVarName(getVarName())) {
            return context.getVarCacheValue(getVarName());
        }
        Object value = doMonitorAction(context, rule);
        context.putVarValue(getNameSpace(), getVarName(), value);
        return value;
    }

    protected Object doMonitorAction(RuleContext context, Rule rule) {
        Object value = null;

        try {
            value = doAction(context, rule);
        } catch (Exception e) {
            LOG.error(
                "Var doMonitorAction doAction error, var is: " + getVarName() + " ,rule is : " + rule.getRuleCode(), e);
            return null;
        }
        return value;

    }

    @Override
    public boolean init() {
        return true;
    }

    public abstract boolean canLazyLoad();

}
