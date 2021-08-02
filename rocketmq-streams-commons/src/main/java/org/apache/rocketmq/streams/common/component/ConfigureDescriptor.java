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
package org.apache.rocketmq.streams.common.component;

public class ConfigureDescriptor {

    private String propertyKey;
    private String defaultValue;
    /**
     * 这个值是否必须用户设置
     */
    private boolean requiredSet = false;
    private String groupName;
    /**
     * 环境变量对应的可以
     */
    private String envPropertyKey;

    public ConfigureDescriptor(String groupName, String propertyKey, String defaultValue, boolean requiredSet,
                               String envPropertyKey) {
        this(groupName, propertyKey, defaultValue, requiredSet);
        this.envPropertyKey = envPropertyKey;
    }

    public ConfigureDescriptor(String groupName, String propertyKey, String defaultValue, boolean requiredSet) {
        this.groupName = groupName;
        this.propertyKey = propertyKey;
        this.defaultValue = defaultValue;
        this.requiredSet = requiredSet;
        this.envPropertyKey = propertyKey;
    }

    public ConfigureDescriptor(String groupName, String propertyKey, boolean requiredSet) {
        this(groupName, propertyKey, null, requiredSet);
    }

    public ConfigureDescriptor(String propertyKey, boolean requiredSet) {
        this(null, propertyKey, requiredSet);
    }

    public ConfigureDescriptor(String propertyKey, boolean requiredSet, String defaultValue) {
        this(null, propertyKey, defaultValue, requiredSet);
    }

    public String getPropertyKey() {
        return propertyKey;
    }

    public void setPropertyKey(String propertyKey) {
        this.propertyKey = propertyKey;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public boolean isRequiredSet() {
        return requiredSet;
    }

    public void setRequiredSet(boolean requiredSet) {
        this.requiredSet = requiredSet;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public String getEnvPropertyKey() {
        return envPropertyKey;
    }

    public void setEnvPropertyKey(String envPropertyKey) {
        this.envPropertyKey = envPropertyKey;
    }
}
