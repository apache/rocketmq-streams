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
package org.apache.rocketmq.streams.filter.contants;

import org.apache.rocketmq.streams.common.channel.sink.ISink;
import org.apache.rocketmq.streams.filter.operator.action.Action;

import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.filter.operator.var.Var;
import org.apache.rocketmq.streams.common.metadata.MetaData;

public enum RuleElementType {
    METADATA(MetaData.TYPE),
    VAR(Var.TYPE),
    RULE(Rule.TYPE),
    EXPRESSION(Expression.TYPE),
    ACTION(Action.TYPE),
    DATASOURCE(ISink.TYPE);
    private String type;

    private RuleElementType(String type) {
        this.type = type;
    }

    public String getType() {
        return this.type;
    }
}
