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
package org.apache.rocketmq.streams.filter.operator.action;

import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.db.driver.JDBCDriver;
import org.apache.rocketmq.streams.filter.operator.var.Var;

public abstract class Action<T> extends BasedConfigurable implements IConfigurableAction<T>, IConfigurable {
    public static final String TYPE = "action";

    private transient Map<String, MetaData> metaDataMap = new HashMap<>();
    private transient volatile Map<String, JDBCDriver> dataSourceMap = new HashMap<>();
    private transient volatile Map<String, Var> varMap = new HashMap<>();

    public Action() {
        setType(TYPE);
    }

    public Map<String, MetaData> getMetaDataMap() {
        return metaDataMap;
    }

    public void setMetaDataMap(Map<String, MetaData> metaDataMap) {
        this.metaDataMap = metaDataMap;
    }

    public Map<String, JDBCDriver> getDataSourceMap() {
        return dataSourceMap;
    }

    public void setDataSourceMap(Map<String, JDBCDriver> dataSourceMap) {
        this.dataSourceMap = dataSourceMap;
    }

    public Map<String, Var> getVarMap() {
        return varMap;
    }

    public void setVarMap(Map<String, Var> varMap) {
        this.varMap = varMap;
    }
}
