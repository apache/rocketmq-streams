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
package org.apache.rocketmq.streams.common.channel.source;

public abstract class AbstractPushSource extends AbstractSource {

    /**
     * 需要提供消息变动的监听,这块需要实现者根据消息队列类型自行实现：
     * 1.当新增分片时，调用addNewSplit方法发送系统通知
     * 2.当分片移走时，removeSplit方法，发送系统通知
     */
    protected abstract boolean hasListenerSplitChanged();
}
