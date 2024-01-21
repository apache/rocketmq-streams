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

import org.apache.rocketmq.streams.common.channel.split.ISplit;

public abstract class AbstractSplitReader implements ISplitReader {
    protected ISplit<?, ?> split;
    protected volatile boolean isInterrupt = false;
    protected volatile boolean isClose = false;

    protected String cursor;

    public AbstractSplitReader(ISplit<?, ?> split) {
        this.split = split;
    }

    @Override @Deprecated public SplitCloseFuture close() {
        interrupt();
        return new SplitCloseFuture(this, split);
    }

    @Override public String getProgress() {
        this.cursor = getCursor();
        return this.cursor;
    }

    public abstract String getCursor();

    @Override public boolean isClose() {
        return isClose;
    }

    @Override public ISplit<?, ?> getSplit() {
        return this.split;
    }

    @Override public boolean isInterrupt() {
        return isInterrupt;
    }

    @Override public void interrupt() {
        isInterrupt = true;
    }

    @Override public void finish() {
        this.isClose = true;
    }
}
