package org.apache.rocketmq.streams.common.enums;

import org.apache.rocketmq.streams.common.topology.model.AbstractStage;
import org.apache.rocketmq.streams.common.topology.stages.FilterChainStage;
import org.apache.rocketmq.streams.common.topology.stages.JoinEndChainStage;
import org.apache.rocketmq.streams.common.topology.stages.JoinStartChainStage;
import org.apache.rocketmq.streams.common.topology.stages.OutputChainStage;
import org.apache.rocketmq.streams.common.topology.stages.ScriptChainStage;
import org.apache.rocketmq.streams.common.topology.stages.SynchronousWindowChainStage;
import org.apache.rocketmq.streams.common.topology.stages.UnionEndChainStage;
import org.apache.rocketmq.streams.common.topology.stages.UnionStartChainStage;

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

    public static String getTypeForStage(AbstractStage stage) {
        if (stage instanceof FilterChainStage) {
            return FILTER.getType();
        } else if (stage instanceof ScriptChainStage) {
            return SCRIPT.getType();
        } else if (stage instanceof JoinStartChainStage || stage instanceof JoinEndChainStage) {
            return JOIN.getType();
        } else if (stage instanceof SynchronousWindowChainStage) {
            return WINDOW.getType();
        } else if (stage instanceof OutputChainStage) {
            return OUTPUT.getType();
        } else if (stage instanceof UnionStartChainStage || stage instanceof UnionEndChainStage) {
            return UNION.getType();
        } else {
            return UNKNOWN.getType();
        }
    }

    public String getType() {
        return type;
    }
}
