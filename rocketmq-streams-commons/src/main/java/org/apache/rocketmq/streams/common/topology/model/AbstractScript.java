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
package org.apache.rocketmq.streams.common.topology.model;

import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.configurable.annotation.Changeable;
import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;

public abstract class AbstractScript<T, C extends AbstractContext> extends BasedConfigurable implements
    IStreamOperator<IMessage, List<IMessage>> {

    /**
     * 存储在script中的数据
     */
    public static final String MESSAGE = "message";

    public static final String TYPE = "operator";

    /**
     * 默认python脚本返回一个json结果，结果中有固定key代表表达式的值
     */
    public static final String EXPRESSION_RESULT = "expression_result";

    protected String lastUpdateTime;

    /**
     * 可能是脚本，可能是文件名或文件路径
     */
    @ENVDependence
    protected String value;

    protected String scriptType;

    /**
     * 获取字段名对应的脚本列表
     *
     * @param fieldName
     * @return
     */
    public abstract List<String> getScriptsByDependentField(String fieldName);

    /**
     * 新变量名和依赖字段的关系
     *
     * @return
     */
    public abstract Map<String, List<String>> getDependentFields();

    /**
     * 存在私有空间，可变化
     */
    @Changeable
    private String message;

    public AbstractScript() {
        setType(TYPE);
    }

    public String getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(String lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getScriptType() {
        return scriptType;
    }

    public void setScriptType(String scriptType) {
        this.scriptType = scriptType;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
