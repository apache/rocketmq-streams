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
import org.apache.rocketmq.streams.connectors.model.QuerySplit;

public abstract class AbstractQueryReader extends AbstractSplitReader {
    protected long startTime;
    protected long dateAdd;
    protected long pollingMinute;
    protected long pageNum ;

    public AbstractQueryReader(ISplit<?, ?> split) {
        super(split);
    }

    @Override public void open() {
        QuerySplit dbSplit = (QuerySplit) split;
        this.startTime = dbSplit.getInitStartTime();
        this.dateAdd = dbSplit.getDateAdd();
        this.pollingMinute = dbSplit.getPollingMinute();
        this.pageNum=dbSplit.getPageNum();

    }

    @Override public boolean next() {
        return startTime + pollingMinute * 60 * 1000 < System.currentTimeMillis();
    }


    @Override public long getDelay() {
        return System.currentTimeMillis() - startTime;
    }

    @Override public long getFetchedDelay() {
        return 0;
    }

    @Override public void seek(String cursor) {
        if (cursor == null) {
            this.cursor = this.startTime + ";" + (pageNum + 10000);
            return;
        }
        this.cursor = cursor;
        String[] values = cursor.split(";");
        this.startTime = Long.parseLong(values[0]);
        this.pageNum = Integer.parseInt(values[1]) - 10000;
    }

    @Override public String getCursor() {
        return this.startTime + ";" + (pageNum + 10000);
    }
}
