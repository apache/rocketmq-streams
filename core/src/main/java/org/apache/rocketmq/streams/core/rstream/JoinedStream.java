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
package org.apache.rocketmq.streams.core.rstream;

import com.fasterxml.jackson.databind.util.BeanUtil;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.rocketmq.streams.core.common.Constant;
import org.apache.rocketmq.streams.core.function.AggregateAction;
import org.apache.rocketmq.streams.core.function.FilterAction;
import org.apache.rocketmq.streams.core.function.KeySelectAction;
import org.apache.rocketmq.streams.core.function.ValueJoinAction;
import org.apache.rocketmq.streams.core.function.supplier.AggregateSupplier;
import org.apache.rocketmq.streams.core.function.supplier.FilterSupplier;
import org.apache.rocketmq.streams.core.function.supplier.JoinWindowAggregateSupplier;
import org.apache.rocketmq.streams.core.function.supplier.WindowAggregateSupplier;
import org.apache.rocketmq.streams.core.running.AbstractProcessor;
import org.apache.rocketmq.streams.core.running.Processor;
import org.apache.rocketmq.streams.core.runtime.operators.JoinType;
import org.apache.rocketmq.streams.core.runtime.operators.StreamType;
import org.apache.rocketmq.streams.core.runtime.operators.WindowInfo;
import org.apache.rocketmq.streams.core.topology.virtual.GraphNode;
import org.apache.rocketmq.streams.core.topology.virtual.ProcessorNode;
import org.apache.rocketmq.streams.core.topology.virtual.ShuffleProcessorNode;
import org.apache.rocketmq.streams.core.util.OperatorNameMaker;

import java.util.List;
import java.util.function.Supplier;

import static org.apache.rocketmq.streams.core.util.OperatorNameMaker.MAP_PREFIX;
import static org.apache.rocketmq.streams.core.util.OperatorNameMaker.NAKED_NODE_PREFIX;

public class JoinedStream<V1, V2> {
    private RStream<V1> leftStream;
    private RStream<V2> rightStream;
    private JoinType joinType;

    public JoinedStream(RStream<V1> leftStream, RStream<V2> rightStream, JoinType joinType) {
        this.leftStream = leftStream;
        this.rightStream = rightStream;
        this.joinType = joinType;
    }

    public <K> Where<K> where(KeySelectAction<K, V1> rightKeySelectAction) {
        return new Where<>(rightKeySelectAction);
    }

    public class Where<K> {
        private KeySelectAction<K, V1> leftKeySelectAction;
        private KeySelectAction<K, V2> rightKeySelectAction;

        public Where(KeySelectAction<K, V1> leftKeySelectAction) {
            this.leftKeySelectAction = leftKeySelectAction;
        }


        public Where<K> equalTo(KeySelectAction<K, V2> rightKeySelectAction) {
            this.rightKeySelectAction = rightKeySelectAction;
            return this;
        }

        public <T> WindowStream<K, T> window(WindowInfo windowInfo) {
            String name = OperatorNameMaker.makeName(NAKED_NODE_PREFIX);
            Supplier<Processor<Object>> supplier = new FilterSupplier<>(value -> true);
            Supplier<Processor<V1>> supplierV1 = new FilterSupplier<>(value -> true);
            Supplier<Processor<V2>> supplierV2 = new FilterSupplier<>(value -> true);
//            Supplier<Processor<T>> supplier = new WindowAggregateSupplier<>(windowInfo, () -> null, createCommonAgg());
            {
//                Pipeline leftStreamPipeline = JoinedStream.this.leftStream.getPipeline();
//                GraphNode leftLastNode = leftStreamPipeline.getLastNode();
//                List<GraphNode> allParent = leftLastNode.getAllParent();
//                String parentName = allParent.get(0).getName();

                GroupedStream<K, V1> leftGroupedStream = JoinedStream.this.leftStream.keyBy(leftKeySelectAction);
                leftGroupedStream.addGraphNode(name, supplierV1);

//                Supplier<Processor<V1>> supplier = new WindowAggregateSupplier<>("join", leftLastNode.getName(), windowInfo, () -> null, createCommonAgg());
//                Supplier<Processor<V1>> supplier = new AggregateSupplier<>("join", leftLastNode.getName(), () -> null, createCommonAgg());
//                ShuffleProcessorNode<T> leftShuffleNode = new ShuffleProcessorNode<>("test", leftLastNode.getName(), supplier);
//
//                leftGroupedStream.addGraphNode(leftShuffleNode);


//                WindowInfo leftWindowInfo = this.copy(windowInfo);
//
//                WindowInfo.JoinStream leftStream = new WindowInfo.JoinStream(JoinedStream.this.joinType, StreamType.LEFT_STREAM);
//                leftWindowInfo.setJoinStream(leftStream);
//
//
//                leftWindow = leftGroupedStream.window(leftWindowInfo);
            }

            {
//                Pipeline rightStreamPipeline = JoinedStream.this.rightStream.getPipeline();
//                GraphNode rightLastNode = rightStreamPipeline.getLastNode();
//                test.addParent(rightLastNode);

//                Supplier<Processor<V2>> supplier = new WindowAggregateSupplier<>("join", rightLastNode.getName(), windowInfo, () -> null, createCommonAgg());
//                Supplier<Processor<V2>> supplier = new AggregateSupplier<>("join", rightLastNode.getName(), () -> null, createCommonAgg());
//                ShuffleProcessorNode<T> rightShuffleNode = new ShuffleProcessorNode<>("test", rightLastNode.getName(), supplier);
//
                GroupedStream<K, V2> rightGroupedStream = JoinedStream.this.rightStream.keyBy(rightKeySelectAction);
                rightGroupedStream.addGraphNode(name, supplierV2);
            }

            Pipeline total = new Pipeline();
            return null;
        }

        private <T> AggregateAction<K, T, T> createCommonAgg() {
            return (key, value, accumulator) -> value;
        }

//        private <T> ValueJoinAction<V1, V2, T> createThreeWayPipeAction() {
//            ValueJoinAction<V1, V2, T> action = new ValueJoinAction<V1, V2, T>() {
//                @Override
//                public T apply(V1 value1, V2 value2) {
//                    return value1 + value2;
//                }
//            };
//
//            return action;
//        }

        private WindowInfo copy(WindowInfo windowInfo) {
            WindowInfo result = new WindowInfo();

            WindowInfo.JoinStream joinStream = windowInfo.getJoinStream();
            WindowInfo.JoinStream stream = new WindowInfo.JoinStream(joinStream.getJoinType(), joinStream.getStreamType());

            result.setJoinStream(stream);
            result.setSessionTimeout(windowInfo.getSessionTimeout());
            result.setWindowType(windowInfo.getWindowType());
            result.setWindowSize(windowInfo.getWindowSize());
            result.setWindowSlide(windowInfo.getWindowSlide());

            return result;
        }


//        public class ThreeWayPipeSupplier<T> implements Supplier<Processor<T>> {
//            private ValueJoinAction<V1, V2, T> valueJoinAction;
//
//            public ThreeWayPipeSupplier(ValueJoinAction<V1, V2, T> valueJoinAction) {
//                this.valueJoinAction = valueJoinAction;
//            }
//
//            @Override
//            public Processor<T> get() {
//                return new ThreeWayProcessor(valueJoinAction);
//            }
//
//            public class ThreeWayProcessor extends AbstractProcessor<T> {
//                private ValueJoinAction<V1, V2, T> valueJoinAction;
//
//                public ThreeWayProcessor(ValueJoinAction<V1, V2, T> valueJoinAction) {
//                    this.valueJoinAction = valueJoinAction;
//                }
//
//                @Override
//                public void process(T data) throws Throwable {
//
//                }
//            }
//
//        }

    }
}
