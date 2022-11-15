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
package org.apache.rocketmq.streams.core.runtime;

import org.apache.rocketmq.streams.core.runtime.operators.WindowState;
import org.apache.rocketmq.streams.core.util.Num;
import org.apache.rocketmq.streams.core.util.User;

public class WindowStateTests {
    public static void main(String[] args) throws Throwable {
        WindowState<Num, User> state = new WindowState<>();
        Num num = new Num();
        num.setNumber(10);
        User user = new User();
        user.setName("zeni");
        state.setKey(num);
        state.setValue(user);

        byte[] bytes = WindowState.windowState2Byte(state);

        WindowState<Num, User> state1 = WindowState.byte2WindowState(bytes);

        System.out.println(state1);
    }

}
