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
package org.apache.rocketmq.streams.core.window.fire;

import org.apache.rocketmq.streams.core.metadata.Data;
import org.apache.rocketmq.streams.core.window.WindowKey;

import java.util.List;

public interface WindowFire<K, V> {

    List<WindowKey> fire(String operatorName, long watermark);


    @SuppressWarnings("unchecked")
    default Data<K, V> convert(Data<?, ?> data) {
        return (Data<K, V>) new Data<>(data.getKey(), data.getValue(), data.getTimestamp(), data.getHeader());
    }
}
