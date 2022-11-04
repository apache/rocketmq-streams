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
package org.apache.rocketmq.streams.common.model;

import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;

public class JobConfigure extends BasedConfigurable {

    /**
     * 任务名称
     */
    private String jobName;

    /**
     * 配置名称
     */
    private String configureName;

    /**
     * 配置类型
     */
    private String configureType;

    /**
     * 配置空间
     */
    private String configureNamespace;

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    @Override public String getConfigureName() {
        return configureName;
    }

    @Override public void setConfigureName(String configureName) {
        this.configureName = configureName;
    }

    public String getConfigureType() {
        return configureType;
    }

    public void setConfigureType(String configureType) {
        this.configureType = configureType;
    }

    public String getConfigureNamespace() {
        return configureNamespace;
    }

    public void setConfigureNamespace(String configureNamespace) {
        this.configureNamespace = configureNamespace;
    }
}

