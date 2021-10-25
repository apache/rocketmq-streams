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
package org.apache.rocketmq.streams.connectors.model;

import org.apache.rocketmq.streams.common.context.MessageOffset;

public class PullMessage<T> {
    protected T message;
    protected MessageOffset messageOffset;

    public T getMessage() {
        return message;
    }

    public void setMessage(T message) {
        this.message = message;
    }

    public MessageOffset getMessageOffset() {
        return messageOffset;
    }

    public void setMessageOffset(MessageOffset messageOffset) {
        this.messageOffset = messageOffset;
    }
    /**
     * 获取offset字符串，通过.把主offset和子offset串接在一起
     * @return
     */
    public String getOffsetStr(){
       return this.messageOffset.getOffsetStr();
    }
    public String getMainOffset() {
        return messageOffset.getMainOffset();
    }
}
