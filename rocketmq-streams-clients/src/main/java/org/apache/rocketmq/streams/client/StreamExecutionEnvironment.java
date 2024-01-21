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
package org.apache.rocketmq.streams.client;

import java.util.Properties;
import org.apache.rocketmq.streams.client.source.DataStreamSource;
import org.apache.rocketmq.streams.common.configuration.JobConfiguration;

public class StreamExecutionEnvironment extends AbstractExecutionEnvironment<StreamExecutionEnvironment> {

    private StreamExecutionEnvironment() {
        super();
    }

    public static StreamExecutionEnvironment getExecutionEnvironment() {
        return StreamExecutionEnvironmentFactory.Instance;
    }

    public DataStreamSource create(String namespace, String jobName, JobConfiguration jobConfiguration) {
        Properties properties = this.getProperties();
        properties.putAll(jobConfiguration.getProperties());
        return new DataStreamSource(namespace, jobName, properties);
    }

    public DataStreamSource create(String namespace, String jobName) {
        return new DataStreamSource(namespace, jobName, this.getProperties());
    }

    private static class StreamExecutionEnvironmentFactory {
        private static final StreamExecutionEnvironment Instance = new StreamExecutionEnvironment();
    }

}
