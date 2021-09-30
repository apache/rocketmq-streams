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
package org.apache.rocketmq.streams.db.sink;

import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.configurable.IAfterConfigurableRefreshListener;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.functions.MultiTableSplitFunction;
import org.apache.rocketmq.streams.db.DynamicMultipleDBSplit;

/**
 * @description
 */
public class DynamicMultipleDBSink extends AbstractMultiTableSink implements IAfterConfigurableRefreshListener {

    private static final long serialVersionUID = -4570659943689358381L;
    String logicTableName;
    String fieldName;

//    transient MultiTableSplitFunction<IMessage> multiTableSplitFunction;

    public DynamicMultipleDBSink(){
    }

    public String getLogicTableName() {
        return logicTableName;
    }

    public void setLogicTableName(String logicTableName) {
        this.logicTableName = logicTableName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public DynamicMultipleDBSink(String url, String userName, String password, String logicTableName, String fieldName) {
        super(url, userName, password);
        this.logicTableName = logicTableName;
        this.fieldName = fieldName;
    }

    @Override
    protected String createTableName(String splitId) {
        return this.multiTableSplitFunction.createTableFromSplitId(splitId);
    }

    @Override
    protected ISplit getSplitFromMessage(IMessage message) {
        return this.multiTableSplitFunction.createSplit(message);
    }


    @Override
    public void doProcessAfterRefreshConfigurable(IConfigurableService configurableService) {

        if(this.multiTableSplitFunction == null){

            this.multiTableSplitFunction = new MultiTableSplitFunction<IMessage>() {
                @Override
                public ISplit createSplit(IMessage message) {
                    return new DynamicMultipleDBSplit(message.getMessageBody().getString(fieldName), logicTableName);
                }
                @Override
                public String createTableFromSplitId(String splitId) {
                    return splitId;
                }
            };

        }


    }
}
