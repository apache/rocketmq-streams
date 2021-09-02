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
package org.apache.rocketmq.streams.common.channel.sink;

import java.util.List;
import org.apache.rocketmq.streams.common.channel.split.ISplit;

public abstract class AbstractSupportShuffleSink extends AbstractSink {

    protected transient int splitNum = 10;//分片个数

    //sls对应的project和logstore初始化是否完成标志
    protected volatile transient boolean hasCreated = false;

    /**
     * 获取sink的主题，在sls中是logStore，RocketMQ是topic
     *
     * @return
     */
    public abstract String getShuffleTopicFieldName();

    /**
     * 创建一个消息队列主题，需要判断是否已经存在，如果已经存在，不需要重复创建
     */
    protected abstract void createTopicIfNotExist(int splitNum);

    /**
     * 获取所有的分片
     *
     * @return
     */
    public abstract List<ISplit> getSplitList();

    @Override
    protected boolean initConfigurable() {
        boolean success = super.initConfigurable();
        hasCreated = false;
        checkAndCreateTopic();
        return success;
    }

    /**
     * 创建主题，只创建一次
     */
    protected void checkAndCreateTopic() {
        if (!hasCreated) {
            synchronized (this) {
                if (!hasCreated) {
                    try {
                        createTopicIfNotExist(splitNum);
                    }catch (Exception e){
                        e.printStackTrace();
                    }

                    hasCreated = true;
                }

            }
        }

    }

    public void setSplitNum(int splitNum) {
        this.splitNum = splitNum;
    }

    public abstract int getSplitNum();
}
