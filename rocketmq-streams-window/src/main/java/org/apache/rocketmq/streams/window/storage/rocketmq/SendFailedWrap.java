package org.apache.rocketmq.streams.window.storage.rocketmq;
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

import org.apache.rocketmq.common.message.Message;

public class SendFailedWrap {
    private String shuffleId;
    private Message message;

    public SendFailedWrap(String shuffleId, Message message) {
        this.shuffleId = shuffleId;
        this.message = message;
    }

    public String getShuffleId() {
        return shuffleId;
    }

    public void setShuffleId(String shuffleId) {
        this.shuffleId = shuffleId;
    }

    public Message getMessage() {
        return message;
    }

    public void setMessage(Message message) {
        this.message = message;
    }
}
