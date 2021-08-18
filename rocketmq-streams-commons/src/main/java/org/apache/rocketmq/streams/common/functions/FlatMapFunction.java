package org.apache.rocketmq.streams.common.functions;

import java.io.Serializable;
import java.util.List;

public interface FlatMapFunction <OUT, IN> extends Function, Serializable {

    List<OUT> flatMap(IN message) throws Exception;
}
