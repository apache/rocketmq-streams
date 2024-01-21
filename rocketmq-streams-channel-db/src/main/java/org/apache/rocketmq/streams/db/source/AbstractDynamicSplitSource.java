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
package org.apache.rocketmq.streams.db.source;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.streams.common.channel.split.CommonSplit;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.connectors.source.AbstractPullSource;

;

public abstract class AbstractDynamicSplitSource extends AbstractPullSource {
    protected int splitCount = 1;

    @Override public List<ISplit<?, ?>> fetchAllSplits() {
        List<ISplit<?, ?>> splits = new ArrayList<>();
        for (int i = 0; i < 1; i++) {
            splits.add(new CommonSplit(i + ""));
        }
        return splits;
    }

    public int getSplitCount() {
        return splitCount;
    }

    public void setSplitCount(int splitCount) {
        this.splitCount = splitCount;
    }
}
