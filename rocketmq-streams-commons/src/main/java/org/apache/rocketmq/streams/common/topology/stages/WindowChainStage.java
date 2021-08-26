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

import org.apache.rocketmq.streams.common.configurable.IAfterConfigurableRefreshListener;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.topology.model.IStageHandle;
import org.apache.rocketmq.streams.common.topology.model.IWindow;

public class WindowChainStage<T extends IMessage> extends AbstractWindowStage<T> implements IAfterConfigurableRefreshListener {

    private static final long serialVersionUID = -6592591896560866562L;

    protected transient IStageHandle handle = new IStageHandle() {

        @Override
        protected IMessage doProcess(IMessage message, AbstractContext context) {
            window.doMessage(message, context);
            if (!window.isSynchronous()) {
                context.breakExecute();
            }

            return message;
        }

        @Override
        public String getName() {
            return WindowChainStage.class.getName();
        }

    };

    public WindowChainStage() {
        super.entityName = "window";
    }

    @Override
    protected IStageHandle selectHandle(T t, AbstractContext context) {
        return handle;
    }

    public IWindow getWindow() {
        return window;
    }

    public void setWindow(IWindow window) {
        this.window = window;
        if (IConfigurable.class.isInstance(window)) {
            setWindowName(window.getConfigureName());
            setLabel(window.getConfigureName());
        }
    }

    @Override
    public boolean isAsyncNode() {
        if (window != null) {
            return !window.isSynchronous();
        }
        return false;
    }

    @Override
    public String getEntityName() {
        return super.entityName;
    }

}
