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
package org.apache.rocketmq.streams.common.topology.stages;

import org.apache.rocketmq.streams.common.configurable.annotation.ConfigurableReference;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.monitor.ConsoleMonitorManager;
import org.apache.rocketmq.streams.common.optimization.fingerprint.PreFingerprint;
import org.apache.rocketmq.streams.common.topology.metric.NotFireReason;
import org.apache.rocketmq.streams.common.topology.model.AbstractRule;
import org.apache.rocketmq.streams.common.utils.TraceUtil;

public class FilterChainStage<T extends IMessage, R extends AbstractRule> extends AbstractStatelessChainStage<T> {
    @ConfigurableReference private R rule;

    public FilterChainStage() {
        setEntityName("filter");

    }

    @Override
    public boolean isAsyncNode() {
        return false;
    }

    @Override
    protected IMessage handleMessage(IMessage message, AbstractContext context) {

        boolean isFire = rule.doMessage(message, context);//component.getService().executeRule(message, context, rule);

        if (!isFire) {
            context.breakExecute();

            if (preFingerprint != null) {
                preFingerprint.addLogFingerprintToSource(message);
            }
        }
        return message;
    }

    protected void setNotReasonToContext(AbstractContext context) {

    }

    protected void traceFailExpression(IMessage message, NotFireReason notFireReason) {
        ConsoleMonitorManager.getInstance().reportOutput(FilterChainStage.this, message, ConsoleMonitorManager.MSG_FILTERED, notFireReason.toString());
        TraceUtil.debug(message.getHeader().getTraceId(), "break rule", notFireReason.toString());
    }

    @Override public void startJob() {
        super.startJob();
        if (this.preFingerprint == null) {
            this.preFingerprint = loadLogFinger();
        }
    }

    public R getRule() {
        return rule;
    }

    public void setRule(R rule) {
        if (rule == null) {
            return;
        }
        this.rule = rule;
        setNameSpace(rule.getNameSpace());

    }

    @Override
    public void setPreFingerprint(PreFingerprint preFingerprint) {
        this.preFingerprint = preFingerprint;
    }
}
