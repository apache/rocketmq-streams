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

import org.apache.rocketmq.streams.core.function.SelectAction;
import org.apache.rocketmq.streams.core.function.ValueJoinAction;
import org.apache.rocketmq.streams.core.function.supplier.AddTagSupplier;
import org.apache.rocketmq.streams.core.function.supplier.JoinAggregateSupplier;
import org.apache.rocketmq.streams.core.function.supplier.JoinWindowAggregateSupplier;
import org.apache.rocketmq.streams.core.running.Processor;
import org.apache.rocketmq.streams.core.window.JoinType;
import org.apache.rocketmq.streams.core.window.StreamType;
import org.apache.rocketmq.streams.core.window.WindowInfo;
import org.apache.rocketmq.streams.core.topology.virtual.GraphNode;
import org.apache.rocketmq.streams.core.topology.virtual.ProcessorNode;
import org.apache.rocketmq.streams.core.util.OperatorNameMaker;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static org.apache.rocketmq.streams.core.util.OperatorNameMaker.ADD_TAG;

public class JoinedStream<V1, V2> {
    private RStream<V1> leftStream;
    private RStream<V2> rightStream;
    private JoinType joinType;

    public JoinedStream(RStream<V1> leftStream, RStream<V2> rightStream, JoinType joinType) {
        this.leftStream = leftStream;
        this.rightStream = rightStream;
        this.joinType = joinType;
    }

    public <K> Where<K> where(SelectAction<K, V1> rightSelectAction) {
        return new Where<>(rightSelectAction);
    }

    public class Where<K> {
        private SelectAction<K, V1> leftSelectAction;
        private SelectAction<K, V2> rightSelectAction;

        public Where(SelectAction<K, V1> leftSelectAction) {
            this.leftSelectAction = leftSelectAction;
        }


        public Where<K> equalTo(SelectAction<K, V2> rightSelectAction) {
            this.rightSelectAction = rightSelectAction;
            return this;
        }

        public <OUT> RStream<OUT> apply(ValueJoinAction<V1, V2, OUT> joinAction) {
            List<String> temp = new ArrayList<>();
            Pipeline leftStreamPipeline = JoinedStream.this.leftStream.getPipeline();
            String jobId = leftStreamPipeline.getJobId();

            String name = OperatorNameMaker.makeName(OperatorNameMaker.JOIN_PREFIX, jobId);
            Supplier<Processor<? super OUT>> supplier = new JoinAggregateSupplier<>(name, joinType, joinAction);
            ProcessorNode<OUT> commChild = new ProcessorNode(name, temp, supplier);


            {
                GroupedStream<K, V1> leftGroupedStream = JoinedStream.this.leftStream.keyBy(leftSelectAction);
                String addTagName = OperatorNameMaker.makeName(ADD_TAG, jobId);
                leftGroupedStream.addGraphNode(addTagName, new AddTagSupplier<>(() -> StreamType.LEFT_STREAM));

                GraphNode lastNode = leftStreamPipeline.getLastNode();
                temp.add(lastNode.getName());
                commChild.addParent(lastNode);
            }

            Pipeline rightStreamPipeline = JoinedStream.this.rightStream.getPipeline();
            String rightJobId = rightStreamPipeline.getJobId();
            if (!Objects.equals(jobId, rightJobId)) {
                throw new IllegalStateException("left stream and right stream must have same jobId.");
            }

            {
                GroupedStream<K, V2> rightGroupedStream = JoinedStream.this.rightStream.keyBy(rightSelectAction);
                String addTagName = OperatorNameMaker.makeName(ADD_TAG, jobId);
                rightGroupedStream.addGraphNode(addTagName, new AddTagSupplier<>(()-> StreamType.RIGHT_STREAM));

                GraphNode lastNode = rightStreamPipeline.getLastNode();
                temp.add(lastNode.getName());
                commChild.addParent(lastNode);

                lastNode.addChild(commChild);
            }
            return new RStreamImpl<>(leftStreamPipeline, commChild);
        }

        public JoinWindow<K> window(WindowInfo windowInfo) {
            return new JoinWindow<>(this.leftSelectAction, this.rightSelectAction, windowInfo);
        }
    }

    public class JoinWindow<K> {
        private SelectAction<K, V1> leftSelectAction;
        private SelectAction<K, V2> rightSelectAction;
        private WindowInfo windowInfo;

        public JoinWindow(SelectAction<K, V1> leftSelectAction, SelectAction<K, V2> rightSelectAction, WindowInfo windowInfo) {
            this.leftSelectAction = leftSelectAction;
            this.rightSelectAction = rightSelectAction;
            this.windowInfo = windowInfo;
        }

        public <OUT> RStream<OUT> apply(ValueJoinAction<V1, V2, OUT> joinAction) {
            List<String> temp = new ArrayList<>();
            WindowInfo.JoinStream joinStream = new WindowInfo.JoinStream(JoinedStream.this.joinType, null);
            windowInfo.setJoinStream(joinStream);

            Pipeline leftStreamPipeline = JoinedStream.this.leftStream.getPipeline();
            String jobId = leftStreamPipeline.getJobId();

            String name = OperatorNameMaker.makeName(OperatorNameMaker.JOIN_WINDOW_PREFIX, jobId);
            Supplier<Processor<? super OUT>> supplier = new JoinWindowAggregateSupplier<>(name, windowInfo, joinAction);
            ProcessorNode<OUT> commChild = new ProcessorNode(name, temp, supplier);


            {
                GroupedStream<K, V1> leftGroupedStream = JoinedStream.this.leftStream.keyBy(leftSelectAction);

                WindowInfo leftWindowInfo = this.copy(windowInfo);

                WindowInfo.JoinStream leftStream = new WindowInfo.JoinStream(JoinedStream.this.joinType, StreamType.LEFT_STREAM);
                leftWindowInfo.setJoinStream(leftStream);

                leftGroupedStream.window(leftWindowInfo);

                GraphNode lastNode = leftStreamPipeline.getLastNode();
                temp.add(lastNode.getName());
                commChild.addParent(lastNode);
            }

            {

                GroupedStream<K, V2> rightGroupedStream = JoinedStream.this.rightStream.keyBy(rightSelectAction);

                WindowInfo rightWindowInfo = this.copy(windowInfo);

                WindowInfo.JoinStream leftStream = new WindowInfo.JoinStream(JoinedStream.this.joinType, StreamType.RIGHT_STREAM);
                rightWindowInfo.setJoinStream(leftStream);

                rightGroupedStream.window(rightWindowInfo);

                Pipeline rightStreamPipeline = JoinedStream.this.rightStream.getPipeline();
                String rightJobId = rightStreamPipeline.getJobId();
                if (!Objects.equals(jobId, rightJobId)) {
                    throw new IllegalStateException("left stream and right stream must have same jobId.");
                }

                GraphNode lastNode = rightStreamPipeline.getLastNode();
                temp.add(lastNode.getName());
                commChild.addParent(lastNode);

                lastNode.addChild(commChild);
            }

            return new RStreamImpl<>(leftStreamPipeline, commChild);
        }

        private WindowInfo copy(WindowInfo windowInfo) {
            WindowInfo result = new WindowInfo();

            WindowInfo.JoinStream joinStream = windowInfo.getJoinStream();

            if (joinStream != null) {
                WindowInfo.JoinStream stream = new WindowInfo.JoinStream(joinStream.getJoinType(), joinStream.getStreamType());
                result.setJoinStream(stream);
            }

            result.setSessionTimeout(windowInfo.getSessionTimeout());
            result.setWindowType(windowInfo.getWindowType());
            result.setWindowSize(windowInfo.getWindowSize());
            result.setWindowSlide(windowInfo.getWindowSlide());

            return result;
        }


    }
}
