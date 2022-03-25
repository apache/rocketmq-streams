package org.apache.rocketmq.streams.running;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class AbstractProcessor<K, V, OK, OV> implements Processor<K, V, OK, OV> {
    private final List<Processor<K, V, OK, OV>> children = new ArrayList<>();

    @Override
    public void addChild(Processor<K, V, OK, OV> processor) {
        children.add(processor);
    }

    protected List<Processor<K, V, OK, OV>> getChildren() {
        return Collections.unmodifiableList(children);
    }

    @Override
    public void close() throws Exception {

    }
}
