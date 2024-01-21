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
package org.apache.rocketmq.streams.common.configurable;

import java.util.Properties;
import org.apache.rocketmq.streams.common.utils.IdUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractConfigurable implements IConfigurable {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractConfigurable.class);

    protected String jobName;
    protected transient Properties configuration = new Properties();
    private String nameSpace;
    private String name;
    private String type;
    private transient boolean hasInit = false;

    @Override public boolean init() {
        boolean initConfigurable = true;
        if (!hasInit) {
            try {
                initConfigurable = initConfigurable();
            } catch (Exception e) {
                LOGGER.error("[{}][{}] init configurable error", IdUtil.instanceId(), getName(), e);
                throw new RuntimeException("init configurable error ", e);
            }
            hasInit = true;
        }
        return initConfigurable;
    }

    @Override public void destroy() {
    }

    protected boolean initConfigurable() {
        return true;
    }

    public boolean isHasInit() {
        return hasInit;
    }

    public void setHasInit(boolean hasInit) {
        this.hasInit = hasInit;
    }

    @Override public String getJobName() {
        return jobName;
    }

    @Override public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    @Override public String getNameSpace() {
        return nameSpace;
    }

    @Override public void setNameSpace(String nameSpace) {
        this.nameSpace = nameSpace;
    }

    @Override public String getName() {
        return name;
    }

    @Override public void setName(String name) {
        this.name = name;
    }

    @Override public String getType() {
        return type;
    }

    @Override public void setType(String type) {
        this.type = type;
    }

    @Override public Properties getConfiguration() {
        return configuration;
    }

    @Override
    public void setConfiguration(Properties configuration) {
        this.configuration = configuration;
    }
}
