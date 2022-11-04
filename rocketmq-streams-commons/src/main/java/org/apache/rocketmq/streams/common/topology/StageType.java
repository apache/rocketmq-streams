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
package org.apache.rocketmq.streams.common.topology;

import org.apache.rocketmq.streams.common.topology.model.AbstractStage;
import org.apache.rocketmq.streams.common.topology.stages.FilterChainStage;
import org.apache.rocketmq.streams.common.topology.stages.JoinEndChainStage;
import org.apache.rocketmq.streams.common.topology.stages.JoinStartChainStage;
import org.apache.rocketmq.streams.common.topology.stages.OutputChainStage;
import org.apache.rocketmq.streams.common.topology.stages.ScriptChainStage;
import org.apache.rocketmq.streams.common.topology.stages.UnionEndChainStage;
import org.apache.rocketmq.streams.common.topology.stages.UnionStartChainStage;
import org.apache.rocketmq.streams.common.topology.stages.WindowChainStage;

public enum StageType {

    SOURCE("source"),
    OUTPUT("output"),
    FILTER("filter"),
    SCRIPT("script"),
    JOIN("join"),
    WINDOW("window"),
    UNION("union"),
    UNKNOWN("unknown"),
    ;

    private final String type;

    StageType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public static String getTypeForStage(AbstractStage stage) {
        if (stage instanceof FilterChainStage) {
            return FILTER.getType();
        } else if (stage instanceof ScriptChainStage) {
            return SCRIPT.getType();
        } else if (stage instanceof JoinStartChainStage || stage instanceof JoinEndChainStage) {
            return JOIN.getType();
        } else if (stage instanceof WindowChainStage) {
            return WINDOW.getType();
        } else if (stage instanceof OutputChainStage) {
            return OUTPUT.getType();
        } else if (stage instanceof UnionStartChainStage || stage instanceof UnionEndChainStage) {
            return UNION.getType();
        } else {
            return UNKNOWN.getType();
        }
    }
}
