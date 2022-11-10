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
package org.apache.rocketmq.streams.examples.joinWindow;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.streams.core.RocketMQStream;
import org.apache.rocketmq.streams.core.function.ValueJoinAction;
import org.apache.rocketmq.streams.core.rstream.RStream;
import org.apache.rocketmq.streams.core.rstream.StreamBuilder;
import org.apache.rocketmq.streams.core.runtime.operators.Time;
import org.apache.rocketmq.streams.core.runtime.operators.WindowBuilder;
import org.apache.rocketmq.streams.core.topology.TopologyBuilder;
import org.apache.rocketmq.streams.core.util.Pair;
import org.apache.rocketmq.streams.examples.pojo.Num;
import org.apache.rocketmq.streams.examples.pojo.User;

import java.util.Properties;

public class JoinWindow {
    public static void main(String[] args) {
        StreamBuilder builder = new StreamBuilder("joinWindow");

        RStream<User> user = builder.source("user", total -> {
            User user1 = JSON.parseObject(total, User.class);
            return new Pair<>(null, user1);
        });

        RStream<Num> num = builder.source("num", source -> {
            Num user12 = JSON.parseObject(source, Num.class);
            return new Pair<>(null, user12);
        });

        user.join(num)
                .where(User::getName)
                .equalTo(Num::getName)
                .window(WindowBuilder.tumblingWindow(Time.seconds(10)))
                .apply(new ValueJoinAction<User, Num, Object>() {
                    @Override
                    public Object apply(User value1, Num value2) {
                        return null;
                    }
                })
                .print();

        TopologyBuilder topologyBuilder = builder.build();

        Properties properties = new Properties();
        properties.put(MixAll.NAMESRV_ADDR_PROPERTY, "127.0.0.1:9876");

        RocketMQStream rocketMQStream = new RocketMQStream(topologyBuilder, properties);

        rocketMQStream.start();
    }
}
