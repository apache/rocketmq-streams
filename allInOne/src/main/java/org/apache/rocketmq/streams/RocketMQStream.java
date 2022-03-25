package org.apache.rocketmq.streams;
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

import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.streams.metadata.StreamConfig;
import org.apache.rocketmq.streams.running.WorkerThread;
import org.apache.rocketmq.streams.topology.TopologyBuilder;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class RocketMQStream {
    private static InternalLogger log = ClientLogger.getLog();
    private final TopologyBuilder topologyBuilder;
    private final Properties properties;


    public RocketMQStream(TopologyBuilder topologyBuilder, Properties properties) {
        this.topologyBuilder = topologyBuilder;
        this.properties = properties;
    }


    public void start() {
        //启动线程
        try {
            int threadNum = StreamConfig.STREAMS_PARALLEL_THREAD_NUM;
            for (int i = 0; i < threadNum; i++) {
                WorkerThread thread = new WorkerThread(topologyBuilder, "127.0.0.1:9876");
                thread.start();
            }
        } catch (Throwable t) {
            //todo
            t.printStackTrace();
        }


        //线程中启动任务client

        //利用client重平衡将启动task

        CountDownLatch count = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(count::countDown));
        try {
            count.await();
        } catch (InterruptedException e) {
            log.error("wait shutdown error.", e);
        }
    }


}
