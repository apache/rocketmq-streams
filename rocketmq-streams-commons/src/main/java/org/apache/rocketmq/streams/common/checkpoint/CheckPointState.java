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

import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.streams.common.context.MessageOffset;

public class CheckPointState {
    protected Map<String, MessageOffset> queueIdAndOffset = new HashMap<>();//存储已经处理完成的队列id和最大的offset
    /**
     * 0：基于queueIdAndOffset作为已经完成的状态；-1，本次状态不反馈，请忽略，此时不做offset保存；1.我已经反馈，但我不存储状态，以其他stage状态为主
     */
    protected int resultType = 0;

    public Map<String, MessageOffset> getQueueIdAndOffset() {
        return queueIdAndOffset;
    }

    public void setQueueIdAndOffset(
        Map<String, MessageOffset> queueIdAndOffset) {
        this.queueIdAndOffset = queueIdAndOffset;
    }

    public boolean isReplyRefuse() {
        return this.resultType == -1;
    }

    public boolean isReplyAnyOny() {
        return this.resultType == 1;
    }

    public void replyRefuse() {
        this.resultType = -1;
    }

    public void replyAnyOne() {
        this.resultType = 1;
    }

    public int getResultType() {
        return resultType;
    }

    public void setResultType(int resultType) {
        this.resultType = resultType;
    }
}
