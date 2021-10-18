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
package org.apache.rocketmq.streams.common.channel.impl;

import org.apache.rocketmq.streams.common.channel.sink.AbstractSink;
import org.apache.rocketmq.streams.common.context.IMessage;

import java.util.ArrayList;
import java.util.List;

/**
 * @description just support json object
 */
public class CollectionSink extends AbstractSink {

    List<Object> data;

    public CollectionSink(){
        data = new ArrayList<>();
    }

    public CollectionSink(List<Object> data){
        this.data = data;
    }

    @Override
    protected synchronized boolean batchInsert(List<IMessage> messages) {
        for(IMessage msg : messages){
            if(msg.isJsonMessage()){
                data.add(msg.getMessageBody().toJSONString());
            }else{
                data.add(msg.getMessageValue());
            }
        }

        return false;
    }

    public List<Object> getData(){
        return data;
    }
}
