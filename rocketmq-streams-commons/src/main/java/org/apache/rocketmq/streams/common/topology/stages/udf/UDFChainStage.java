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
package org.apache.rocketmq.streams.common.topology.stages.udf;

import org.apache.rocketmq.streams.common.checkpoint.CheckPointMessage;
import org.apache.rocketmq.streams.common.configurable.IAfterConfigurableRefreshListener;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.topology.model.IStageHandle;
import org.apache.rocketmq.streams.common.topology.stages.AbstractStatelessChainStage;
import org.apache.rocketmq.streams.common.utils.Base64Utils;
import org.apache.rocketmq.streams.common.utils.InstantiationUtil;

/**
 * 所有给用户自定义代码的通用类，会转化成这个stage
 */
public class UDFChainStage extends AbstractStatelessChainStage implements IAfterConfigurableRefreshListener {
    protected String udfOperatorClassSerializeValue;//用户自定义的operator的序列化字节数组，做了base64解码
    protected transient StageBuilder selfChainStage;


    public UDFChainStage() {}

    public UDFChainStage(StageBuilder selfOperator) {
        this.selfChainStage = selfOperator;
        byte[] bytes = InstantiationUtil.serializeObject(selfOperator);
        udfOperatorClassSerializeValue = Base64Utils.encode(bytes);
    }

    @Override
    public void checkpoint(IMessage message, AbstractContext context, CheckPointMessage checkPointMessage) {
        selfChainStage.checkpoint(message, context, checkPointMessage);
    }

    @Override
    public boolean isAsyncNode() {
        return false;
    }

    @Override
    protected IStageHandle selectHandle(IMessage message, AbstractContext context) {
        return selfChainStage.selectHandle(message, context);
    }

    @Override public IMessage doMessage(IMessage message, AbstractContext context) {
        super.doMessage(message, context);
        if(!context.isContinue()&&this.filterFieldNames!=null&&context.get("_logfinger")!=null){
            addLogFingerprintToSource(message);
        }
        if(context.get("NEED_USE_FINGER_PRINT")!=null){
            context.remove("NEED_USE_FINGER_PRINT");
        }
        return message;
    }

    public String getUdfOperatorClassSerializeValue() {
        return udfOperatorClassSerializeValue;
    }

    public void setUdfOperatorClassSerializeValue(String udfOperatorClassSerializeValue) {
        this.udfOperatorClassSerializeValue = udfOperatorClassSerializeValue;
    }

    @Override
    public void doProcessAfterRefreshConfigurable(IConfigurableService configurableService) {
        byte[] bytes = Base64Utils.decode(udfOperatorClassSerializeValue);
        selfChainStage = InstantiationUtil.deserializeObject(bytes);
        loadLogFinger();
    }



}
