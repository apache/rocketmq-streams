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
package org.apache.rocketmq.streams.common.monitor.impl;

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.monitor.IMonitor;

import java.util.List;

public class NothingMontior implements IMonitor {
    @Override
    public IMonitor createChildren(String... childrenName) {
        return new NothingMontior();
    }

    @Override
    public IMonitor createChildren(IConfigurable configurable) {
        return null;
    }

    @Override
    public IMonitor startMonitor(String name) {
        return this;
    }

    @Override
    public IMonitor endMonitor() {
        return this;
    }

    @Override
    public boolean isSlow() {
        return false;
    }

    @Override
    public boolean isError() {
        return false;
    }

    @Override
    public IMonitor occureError(Exception e, String... messages) {
        return this;
    }

    @Override
    public IMonitor addContextMessage(Object value) {
        return this;
    }

    @Override
    public IMonitor setResult(Object value) {
        return null;
    }

    @Override
    public JSONObject setSampleData(AbstractContext context) {
        return null;
    }

    @Override
    public JSONObject report(String level) {
        return null;
    }

    @Override
    public void output() {

    }

    @Override
    public List<IMonitor> getChildren() {
        return null;
    }

    @Override
    public long getCost() {
        return 0;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public String getType() {
        return null;
    }

    @Override
    public void setType(String type) {
        // TODO Auto-generated method stub

    }
}
