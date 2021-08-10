package org.apache.rocketmq.streams.common.functions;

import java.io.Serializable;
import java.util.List;

public interface FlatMapFunction <T, O> extends Function, Serializable {

    List<T> flatMap(O message) throws Exception;
}
