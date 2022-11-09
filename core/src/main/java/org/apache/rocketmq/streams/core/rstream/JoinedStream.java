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

import org.apache.rocketmq.streams.core.common.Constant;
import org.apache.rocketmq.streams.core.function.AggregateAction;
import org.apache.rocketmq.streams.core.function.KeySelectAction;
import org.apache.rocketmq.streams.core.runtime.operators.JoinType;
import org.apache.rocketmq.streams.core.runtime.operators.WindowInfo;
import org.apache.rocketmq.streams.core.util.CommonNameMaker;

import java.util.Properties;

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

        public WindowStream<K, V1> window(WindowInfo windowInfo) {
            //左右侧流进入同一个topic，相同key进入同一个queue
            GroupedStream<K, V1> leftGroupedStream = JoinedStream.this.leftStream.keyBy(leftKeySelectAction);
            GroupedStream<K, V2> rightGroupedStream = JoinedStream.this.rightStream.keyBy(rightKeySelectAction);

            //只保证window算子后的第一个算子，左右流具有相同的name
            Properties properties = new Properties();
            properties.put(Constant.COMMON_NAME_MAKER, new CommonNameMaker("windowJoin"));

            WindowStream<K, V1> leftWindow = leftGroupedStream.window(windowInfo);
            leftWindow.setProperties(properties);
            //右侧的流把KV保存到RocksDB中就好
            WindowStream<K, V2> rightWindow = rightGroupedStream.window(windowInfo);
            rightWindow.setProperties(properties);

            rightWindow.aggregate(new AggregateAction<K, V2, V2>() {
                @Override
                public V2 calculate(K key, V2 value, V2 accumulator) {
                    return value;
                }
            });



            return leftWindow;
        }
    }
}
