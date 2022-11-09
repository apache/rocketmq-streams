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
package org.apache.rocketmq.streams.examples;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.rocketmq.streams.core.runtime.operators.WindowState;
import org.apache.rocketmq.streams.examples.pojo.Num;

@SuppressWarnings("unchecked")
public class serDe {
    private static ObjectMapper objectMapper = new ObjectMapper();
    public static void main(String[] args) throws Throwable {
        Num num = new Num("倪泽",1);

        WindowState<String, Num> state = new WindowState<>("test", num, 10L);

//        byte[] jsonBytes = JSON.toJSONBytes(state, SerializerFeature.WriteClassName);
//
//
//        Object result = JSON.parseObject(jsonBytes, state.getClass());

        byte[] bytes = objectMapper.writeValueAsBytes(state);

        WindowState<String, Num> windowState = (WindowState<String, Num>)objectMapper.readValue(bytes, state.getClass());

        System.out.println(windowState);
    }
}
