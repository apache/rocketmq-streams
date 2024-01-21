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
package org.apache.rocketmq.streams.common.topology;

import org.apache.rocketmq.streams.common.configurable.IConfigurable;

/**
 * Window Definition 处理数据并输出
 */
public interface IWindow
    extends IConfigurable, IShuffleKeyGenerator {

    int DEFAULTFIRE_MODE = 0;// fire time=endtime+watermark
    int MULTI_WINDOW_INSTANCE_MODE = 1;//  fire at window size interval， until event time >endtime+watermark, every window result is independent
    int INCREMENT_FIRE_MODE = 2;//  fire at window size interval， until event time >endtime+watermark, every window result is based preview window result

    /**
     * split char between function
     */
    String SCRIPT_SPLIT_CHAR = ";";

    /**
     * tumble window type
     */
    String TUMBLE_WINDOW = "tumble";

    /**
     * hop window type
     */
    String HOP_WINDOW = "hop";

    String SESSION_WINDOW = "session";

    /**
     * hop window type
     */

    String TYPE = "window";

    /**
     * the delay time of system (ms)
     */
    Integer SYS_DELAY_TIME = 3000;

//    /**
//     * 窗口触发后，需要执行的逻辑
//     *
//     * @param receiver
//     */
//    void setFireReceiver(SectionPipeline receiver);

    /**
     * 是否是同步操作，目前只有over 特殊场景会是true
     *
     * @return
     */
    boolean isSynchronous();

    void start();

//    IWindowCheckpoint getWindowCache();
//
//    void windowInit();
//
//    interface IWindowCheckpoint extends ISink<org.apache.rocketmq.streams.common.channel.sink.AbstractSink> {
//        void finishBatchMsg(BatchFinishMessage batchFinishMessage);
//    }
}
