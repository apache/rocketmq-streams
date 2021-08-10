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

import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configurable.IAfterConfiguableRefreshListerner;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;
import org.apache.rocketmq.streams.common.topology.model.AbstractScript;
import org.apache.rocketmq.streams.common.topology.model.IStageHandle;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.common.utils.TraceUtil;

public class ScriptChainStage<T extends IMessage> extends AbstractStatelessChainStage<T> implements IAfterConfiguableRefreshListerner {
    private static final Log LOG = LogFactory.getLog(ScriptChainStage.class);
    protected String scriptName;

    protected transient AbstractScript script;

    public ScriptChainStage() {
        setEntityName("operator");
    }

    protected transient IStageHandle handle = new IStageHandle() {
        @Override
        protected IMessage doProcess(IMessage message, AbstractContext context) {
            IStreamOperator<IMessage, List<IMessage>> receiver = (IStreamOperator)script;
            List<IMessage> messages = receiver.doMessage(message, context);
            TraceUtil.debug(message.getHeader().getTraceId(), "ScriptChainStage", script.getValue(),
                message.getMessageBody().toJSONString());
            if (messages == null || messages.size() == 0) {
                context.breakExecute();
                return message;
            } else if (messages.size() == 1) {
                context.setMessage(messages.get(0));
            } else {
                context.setSplitModel(true);
                context.setSplitMessages(messages);
            }
            if (ComponentCreator.getPropertyBooleanValue("debug.message.parse")) {
                LOG.info("message size is " + messages.size() + " split mode is " + context.isSplitModel());
            }
            return message;
        }

        @Override
        public String getName() {
            return ScriptChainStage.class.getName();
        }
    };

    @Override
    public void doProcessAfterRefreshConfigurable(IConfigurableService configurableService) {
        if (StringUtil.isEmpty(scriptName)) {
            return;
        }
        AbstractScript script = queryScript(configurableService, AbstractScript.TYPE, scriptName);
        this.script = script;
    }

    @Override
    protected IStageHandle selectHandle(T t, AbstractContext context) {
        return handle;
    }

    protected AbstractScript queryScript(IConfigurableService configurableService, String type, String scriptName) {
        return configurableService.queryConfigurable(type, scriptName);
    }

    public void setScript(AbstractScript script) {
        this.script = script;
        this.setNameSpace(script.getNameSpace());
        this.setScriptName(script.getConfigureName());
        this.setLabel(script.getConfigureName());
    }

    public List<String> getDependentScripts(String fieldName) {
        if (this.script == null) {
            return null;
        }
        if (!fieldName.startsWith("__")) {
            return null;
        }
        return script.getScriptsByDependentField(fieldName);
    }

    @Override
    public boolean isAsyncNode() {
        return false;
    }

    public AbstractScript getScript() {
        return script;
    }

    public String getScriptName() {
        return scriptName;
    }

    public void setScriptName(String scriptName) {
        this.scriptName = scriptName;
    }
}
