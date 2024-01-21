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
package org.apache.rocketmq.streams.dispatcher.strategy;

import org.apache.rocketmq.streams.dispatcher.IStrategy;
import org.apache.rocketmq.streams.dispatcher.enums.DispatchMode;

public class StrategyFactory {

    public static IStrategy getStrategy(DispatchMode mode) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        Class<IStrategy> strategyClass = (Class<IStrategy>) StrategyFactory.class.getClassLoader().loadClass("org.apache.rocketmq.streams.dispatcher.strategy." + mode.getName() + "Strategy");
        return strategyClass.newInstance();
    }

}
