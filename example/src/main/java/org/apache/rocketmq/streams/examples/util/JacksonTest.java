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
package org.apache.rocketmq.streams.examples.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.rocketmq.streams.core.function.ValueJoinAction;
import org.apache.rocketmq.streams.core.runtime.operators.WindowState;
import org.apache.rocketmq.streams.core.typeUtil.TypeExtractor;
import org.apache.rocketmq.streams.core.typeUtil.TypeWrapper;
import org.apache.rocketmq.streams.core.util.Utils;
import org.apache.rocketmq.streams.examples.pojo.Num;
import org.apache.rocketmq.streams.examples.pojo.Union;
import org.apache.rocketmq.streams.examples.pojo.User;

public class JacksonTest {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) throws Throwable {
//        User nize = new User("nize", 23);
//        WindowState<String, User> state = new WindowState<>("testKey",nize, 1L);
//
//        byte[] bytes = Utils.object2Byte(state);
//
//        Class<?> temp = WindowState.class;
//        Class<WindowState<String, User>> type = (Class<WindowState<String, User>>) temp;
//
//        TypeReference<WindowState<String, User>> typeReference = new TypeReference<WindowState<String, User>>() {};
//        WindowState<String, User> windowState =  objectMapper.readValue(bytes, type);
//
//        System.out.println(windowState);

        ValueJoinAction<User, Num, Union> action = (value1, value2) -> {
            if (value1 != null && value2 != null) {
                System.out.println("name in user: " + value1.getName());
                System.out.println("name in num: " + value2.getName());

                return new Union(value1.getName(), value1.getAge(), value2.getNum());
            }

            if (value2 != null) {
                System.out.println("name in num: " + value2.getName());
                return new Union(value2.getName(), 0, value2.getNum());
            }


            if (value1 != null) {
                System.out.println("name in num: " + value1.getName());
                return new Union(value1.getName(), value1.getAge(), 0);
            }

            throw new IllegalStateException();
        };


        ValueJoinAction<User, Num, Union> joinAction = new ValueJoinAction<User, Num, Union>() {
            @Override
            public Union apply(User value1, Num value2) {
                if (value1 != null && value2 != null) {
                    System.out.println("name in user: " + value1.getName());
                    System.out.println("name in num: " + value2.getName());

                    return new Union(value1.getName(), value1.getAge(), value2.getNum());
                }

                if (value2 != null) {
                    System.out.println("name in num: " + value2.getName());
                    return new Union(value2.getName(), 0, value2.getNum());
                }


                if (value1 != null) {
                    System.out.println("name in num: " + value1.getName());
                    return new Union(value1.getName(), value1.getAge(), 0);
                }

                throw new IllegalStateException();
            }
        };

        TypeWrapper typeWrapper = TypeExtractor.firstParameterSmart(joinAction, "apply");

        System.out.println(typeWrapper);
    }
}
