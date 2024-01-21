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
package org.apache.rocketmq.streams.common.model;

import org.apache.rocketmq.streams.common.channel.source.ISource;

/**
 * 在sql编译时，需要把source放到上下文中，因为window shuffle需要根据source的分片来创建shuffle 队列
 */
public class SQLCompileContextForSource extends ThreadLocal<ISource> {

    private static final SQLCompileContextForSource instance = new SQLCompileContextForSource();

    private SQLCompileContextForSource() {
    }

    public static SQLCompileContextForSource getInstance() {
        return instance;
    }
}