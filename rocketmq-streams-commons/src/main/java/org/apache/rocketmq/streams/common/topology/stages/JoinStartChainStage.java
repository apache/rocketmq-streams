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

import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.MessageHeader;
import org.apache.rocketmq.streams.common.topology.model.IStageHandle;

public class JoinStartChainStage extends EmptyChainStage {
    protected String rightDependentTableName;
    protected String leftLableName;
    protected String rightLableName;
    @Override
    protected IMessage proccessMessage(IMessage message, AbstractContext context) {
        String lable = message.getHeader().getMsgRouteFromLable();
        if (lable != null) {
            if (lable.equals(rightDependentTableName)) {
                message.getHeader().addRouteLabel(rightLableName);
            } else {
                message.getHeader().addRouteLabel(leftLableName);
            }

        } else {
            throw new RuntimeException("can not dipatch message, need route label " + toJson());
        }
        return message;
    }

    public String getRightDependentTableName() {
        return rightDependentTableName;
    }

    public void setRightDependentTableName(String rightDependentTableName) {
        this.rightDependentTableName = rightDependentTableName;
    }

    public String getLeftLableName() {
        return leftLableName;
    }

    public void setLeftLableName(String leftLableName) {
        this.leftLableName = leftLableName;
    }

    public String getRightLableName() {
        return rightLableName;
    }

    public void setRightLableName(String rightLableName) {
        this.rightLableName = rightLableName;
    }
}
