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
import org.apache.rocketmq.streams.core.runtime.operators.WindowState;
import org.apache.rocketmq.streams.core.util.Utils;
import org.apache.rocketmq.streams.examples.pojo.User;

public class JacksonTest {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) throws Throwable {
        User nize = new User("nize", 23);
        WindowState<Object, Object> state = new WindowState<>("testKey",nize, 1L);

        byte[] bytes = Utils.object2Byte(state);

        Class<?> temp = WindowState.class;
        Class<WindowState<String, User>> type = (Class<WindowState<String, User>>) temp;
        TypeReference<WindowState<String, User>> typeReference = new TypeReference<WindowState<String, User>>() {
        };
        WindowState<String, User> windowState =  objectMapper.readValue(bytes, typeReference);

        System.out.println(windowState);


    }
}
