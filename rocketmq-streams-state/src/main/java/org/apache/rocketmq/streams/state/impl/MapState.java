package org.apache.rocketmq.streams.state.impl;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.state.AbstractState;
import org.apache.rocketmq.streams.state.IEntryProcessor;
import org.apache.rocketmq.streams.state.backend.IStateBackend;

public class MapState<V> extends AbstractState<String,V> {

    public MapState(String namespace, String backendName) {
        super(namespace, backendName);
    }
}
