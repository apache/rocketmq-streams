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
package org.apache.rocketmq.streams.common.configuration;

public class EmitInsertConfig {
    protected Long emitBeforeValue;
    protected Long emitAfterValue;
    protected Long emitMaxDelay;

    public Long getEmitBeforeValue() {
        return emitBeforeValue;
    }

    public void setEmitBeforeValue(long value) {
        this.emitBeforeValue = value;
    }

    public Long getEmitAfterValue() {
        return this.emitAfterValue;
    }

    public void setEmitAfterValue(long value) {
        this.emitAfterValue = value;
    }

    public Long getEmitMaxDelay() {
        return emitMaxDelay;
    }

    public void setEmitMaxDelay(Long emitMaxDelay) {
        this.emitMaxDelay = emitMaxDelay;
    }
}
