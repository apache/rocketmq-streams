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
package org.apache.rocketmq.streams.client.strategy;

import java.util.Properties;
import org.apache.rocketmq.streams.common.component.AbstractComponent;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configure.ConfigureFileKey;

public class WindowStrategy implements Strategy {

    private final Properties properties;

    private WindowStrategy() {
        properties = new Properties();
    }

    private WindowStrategy(String url, String username, String password) {
        properties = new Properties();
        properties.put(AbstractComponent.JDBC_DRIVER, AbstractComponent.DEFAULT_JDBC_DRIVER);
        properties.put(AbstractComponent.JDBC_URL, url);
        properties.put(AbstractComponent.JDBC_USERNAME, username);
        properties.put(AbstractComponent.JDBC_PASSWORD, password);
        properties.put(AbstractComponent.JDBC_TABLE_NAME, AbstractComponent.DEFAULT_JDBC_TABLE_NAME);
    }

    @Override
    public Properties getStrategyProperties() {
        return this.properties;
    }

    public static Strategy exactlyOnce(String url, String username, String password) {
        return new WindowStrategy(url, username, password);
    }

    public static Strategy highPerformance() {

        return new WindowStrategy();
    }

    public static Strategy windowDefaultSiZe(int defualtSize) {
        ComponentCreator.getProperties().put(ConfigureFileKey.DIPPER_WINDOW_DEFAULT_INERVAL_SIZE, defualtSize);
        return null;
    }

}
