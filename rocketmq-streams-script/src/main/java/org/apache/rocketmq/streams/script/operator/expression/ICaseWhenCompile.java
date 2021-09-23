package org.apache.rocketmq.streams.script.operator.expression;

import java.util.List;
import org.apache.rocketmq.streams.common.topology.model.AbstractRule;

public interface ICaseWhenCompile {

    List<? extends AbstractRule> compile(GroupScriptExpression groupScriptExpression);
}
