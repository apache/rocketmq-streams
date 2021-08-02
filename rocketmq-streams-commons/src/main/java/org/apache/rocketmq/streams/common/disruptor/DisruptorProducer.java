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
package org.apache.rocketmq.streams.common.disruptor;

import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DisruptorProducer<T> {
    private static final Log LOG = LogFactory.getLog(DisruptorProducer.class);
    private Disruptor disruptor;

    public DisruptorProducer(Disruptor disruptor) {
        this.disruptor = disruptor;
    }

    public void publish(T data, BufferFullFunction function, boolean discard) {
        RingBuffer<DisruptorEvent> ringBuffer = disruptor.getRingBuffer();
        long sequence;
        try {
            sequence = !discard ? ringBuffer.next() : ringBuffer.tryNext();
        } catch (InsufficientCapacityException e) {
            LOG.warn("ring is full " + e);
            function.process(data);
            return;
        }
        // LOG.info("seq " + sequence + " remaining size " + disruptor.getRingBuffer().remainingCapacity());
        try {
            DisruptorEvent event = ringBuffer.get(sequence);
            event.setData(data);
        } finally {
            ringBuffer.publish(sequence);
        }
    }
}