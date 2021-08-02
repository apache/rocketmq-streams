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
package org.apache.rocketmq.streams.filter.context;

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.streams.filter.monitor.rule.MessageMonitor;
import org.apache.rocketmq.streams.common.context.Message;

public class RuleMessage extends Message {

    protected MessageMonitor messageMonitor;

    public RuleMessage(JSONObject message) {
        super(message);
        messageMonitor = createMessageMonitor(message.toJSONString());
    }

    public MessageMonitor getMessageMonitor() {
        return messageMonitor;
    }

    /**
     * 创建一条信息的监控对象
     *
     * @param message
     * @return
     */
    protected MessageMonitor createMessageMonitor(String message) {
        MessageMonitor messageMonitor = new MessageMonitor();
        messageMonitor.begin(message);
        return messageMonitor;
    }

}
