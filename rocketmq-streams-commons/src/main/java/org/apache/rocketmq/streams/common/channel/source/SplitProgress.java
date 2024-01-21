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
package org.apache.rocketmq.streams.common.channel.source;

public class SplitProgress {
    protected String splitId;
    protected Long progress;
    protected boolean isTimeStamp;
    protected boolean isLogCount;

    public SplitProgress(String splitId, int progressInt, boolean isTimeStamp) {
        this(splitId, (progressInt < 0 ? 0 : Long.valueOf(progressInt)), isTimeStamp);
    }

    public SplitProgress(String splitId, long progress, boolean isTimeStamp) {
        this.splitId = splitId;
        if (isTimeStamp) {
            this.isTimeStamp = true;
            this.isLogCount = false;
        } else {
            this.isTimeStamp = false;
            this.isLogCount = true;
        }
        this.progress = progress;
    }

    public Long getProgress() {
        return progress;
    }

    public void setProgress(Long progress) {
        this.progress = progress;
    }

    public boolean isTimeStamp() {
        return isTimeStamp;
    }

    public void setTimeStamp(boolean timeStamp) {
        isTimeStamp = timeStamp;
    }

    public boolean isLogCount() {
        return isLogCount;
    }

    public void setLogCount(boolean logCount) {
        isLogCount = logCount;
    }

    public String getSplitId() {
        return splitId;
    }
}
