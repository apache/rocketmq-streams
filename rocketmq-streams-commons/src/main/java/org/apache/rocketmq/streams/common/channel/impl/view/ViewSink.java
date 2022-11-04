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

package org.apache.rocketmq.streams.common.channel.impl.view;

import java.util.List;
import org.apache.rocketmq.streams.common.channel.sink.AbstractSink;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ViewSink extends AbstractSink {
    private static final Logger LOG = LoggerFactory.getLogger(ViewSink.class);

    protected String viewTableName;
    @Override protected boolean batchInsert(List<IMessage> messages) {
        return false;
    }

    public String getViewTableName() {
        return viewTableName;
    }

    public void setViewTableName(String viewTableName) {
        this.viewTableName = viewTableName;
    }
}
