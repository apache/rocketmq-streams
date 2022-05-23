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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.Context;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;

public class UnionStartChainStage extends EmptyChainStage {

    //每个pipline，对应一个消息来源，在消息头上会有消息来源的name，根据name转发数据
    protected Map<String, Set<String>> msgSource2StageLables;


    @Override
    protected IMessage proccessMessage(IMessage message, AbstractContext context) {
        if (CollectionUtil.isEmpty(msgSource2StageLables)) {
            return message;
        }
        String msgSourceName = message.getHeader().getMsgRouteFromLable();
        Set<String> lableNames=msgSource2StageLables.get(msgSourceName);
        if(lableNames!=null){
            for(String lableName:lableNames){
                message.getHeader().addRouteLabel(lableName);
            }
        }

        return message;
    }

    public Map<String, Set<String>> getMsgSource2StageLables() {
        return msgSource2StageLables;
    }

    public void setMsgSource2StageLables(Map<String, Set<String>> msgSource2StageLables) {
        this.msgSource2StageLables = msgSource2StageLables;
    }
}
