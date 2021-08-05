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
package org.apache.rocketmq.streams.common.checkpoint;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;
import org.apache.rocketmq.streams.common.interfaces.ISystemMessage;

public class CheckPointMessage implements ISystemMessage {
    protected ISource source;//数据源对象
    protected IStreamOperator streamOperator;//当前的pipline
    protected List<CheckPointState> checkPointStates = new ArrayList<>();
    protected boolean isValidate = true;

    public ISource getSource() {
        return source;
    }

    public void setSource(ISource source) {
        this.source = source;
    }

    public IStreamOperator getStreamOperator() {
        return streamOperator;
    }

    public void setStreamOperator(IStreamOperator streamOperator) {
        this.streamOperator = streamOperator;
    }

    public List<CheckPointState> getCheckPointStates() {
        return checkPointStates;
    }

    public void setCheckPointStates(
        List<CheckPointState> checkPointStates) {
        this.checkPointStates = checkPointStates;
    }

    public void reply(CheckPointState checkPointState) {
        checkPointStates.add(checkPointState);
    }

    public void replyAnyone() {

    }

    public void replyRefuse() {
        isValidate = false;
    }

    public boolean isValidate() {
        return isValidate;
    }
}
