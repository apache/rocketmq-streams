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
package org.apache.rocketmq.streams.script.context;

import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.model.ThreadContext;
import org.apache.rocketmq.streams.script.function.service.IFunctionService;
import org.apache.rocketmq.streams.script.function.service.impl.ScanFunctionService;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

/**
 * 脚本执行的上下文
 */
public class FunctionContext<T extends IMessage>
    extends AbstractContext<T> {

    protected transient IFunctionService functionService = ScanFunctionService.getInstance();

    public FunctionContext(T message) {
        super(message);
    }

    @Override
    public T breakExecute() {
        isContinue = false;
        return message;
    }

    @Override
    public T getMessage() {
        return message;
    }

    @Override
    public AbstractContext copy() {
        IMessage message = this.message.deepCopy();
        FunctionContext context = new FunctionContext(message);
        super.copyProperty(context);
        context.setFunctionService(this.functionService);
        context.setConfigurableService(this.configurableService);
        return context;
    }

    public <T> T executeFunction(String functionName, IMessage message, Object... parameters) {
        boolean contains = functionService.startWith(functionName, IMessage.class, this.getClass());
        if (contains) {
            return this.functionService.executeFunction(message, this, functionName, parameters);
        } else {
            ThreadContext threadContext = ThreadContext.getInstance();
            threadContext.set(this);
            for (int i = 0; i < parameters.length; i++) {
                Object ori = parameters[i];
                parameters[i] = FunctionUtils.getValue(message, this, ori.toString());
                if (parameters[i] == null) {
                    parameters[i] = ori;
                }
            }
            T t = this.functionService.directExecuteFunction(functionName, parameters);
            threadContext.remove();
            return t;
        }

    }

    public IFunctionService getFunctionService() {
        return functionService;
    }

    public void setFunctionService(IFunctionService functionService) {
        this.functionService = functionService;
    }
}
