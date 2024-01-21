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
package org.apache.rocketmq.streams.script.service.udf;

public class SimpleUDAFScript extends UDAFScript {

    public SimpleUDAFScript() {
        this.accumulateMethodName = "accumulate";
        this.createAccumulatorMethodName = "createAccumulator";
        this.getValueMethodName = "getValue";
        this.retractMethodName = "retract";
        this.mergeMethodName = "merge";
        this.methodName = "eval";
        this.initMethodName = "open";
        this.initParameters = new Object[0];
    }

    @Override protected Object createMergeParamters(Iterable its) {
        return its;
    }
}
