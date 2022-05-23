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
package org.apache.rocketmq.streams.connectors.reader;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.connectors.model.PullMessage;

public interface ISplitReader extends Serializable {

    /**
     * Open.
     *
     * @param split the split
     * @throws IOException the io exception
     */
    void open(ISplit split);

    /**
     * Next boolean.
     *
     * @return the boolean
     * @throws IOException          the io exception
     * @throws InterruptedException the interrupted exception
     */
    boolean next();

    /**
     * Gets message.
     *
     * @return the message
     */
    List<PullMessage> getMessage();

    /**
     * Close.
     *
     * @throws IOException the io exception
     */
    SplitCloseFuture close();

    /**
     * Seek.
     *
     * @param cursor the cursor
     * @throws IOException the io exception
     */
    void seek(String cursor);

    /**
     * Gets progress.
     *
     * @return the progress
     * @throws IOException the io exception
     */
    String getProgress();

    /**
     * Get message delay (millseconds)
     *
     * @return delay
     */
    long getDelay();

    /**
     * Get message delay (millseconds) from being fetched
     *
     * @return delay
     */
    long getFetchedDelay();

    boolean isClose();

    ISplit getSplit();

    boolean isInterrupt();

    boolean interrupt();

}
