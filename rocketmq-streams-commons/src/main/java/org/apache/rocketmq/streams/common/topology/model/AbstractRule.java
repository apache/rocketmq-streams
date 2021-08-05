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

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;
import org.apache.rocketmq.streams.common.utils.StringUtil;

public abstract class AbstractRule extends BasedConfigurable implements IStreamOperator<IMessage, Boolean> {

    public static final String TYPE = "rule";

    public static final String FIRE_RULES = "fireRules";

    /**
     * 这个字段以前存在remark中，现在会在映射时做兼容转换，建议后续直接存储在这个字段
     */
    private String msgMetaDataName;

    protected List<String> varNames = new ArrayList<String>();

    protected List<String> actionNames = new ArrayList<String>();

    @Deprecated
    protected List<String> scriptNames = new ArrayList<String>();

    protected String expressionName;

    /**
     * 规则编号
     */
    protected String ruleCode;

    /**
     * 规则描述
     */
    protected String ruleDesc;

    /**
     * 规则标题
     */
    protected String ruleTitle;

    /**
     * 规则发布状态
     */
    protected Integer ruleStatus = 0;

    /**
     * 风险级别
     */
    private String ruleLevel;

    public abstract Set<String> getDependentFields();

    //
    //public abstract Set<String> getFunctionNames();

    public AbstractRule() {
        setType(TYPE);
    }

    public String getMsgMetaDataName() {
        return msgMetaDataName;
    }

    public void setMsgMetaDataName(String msgMetaDataName) {
        this.msgMetaDataName = msgMetaDataName;
    }

    public List<String> getVarNames() {
        return varNames;
    }

    public void setVarNames(List<String> varNames) {
        this.varNames = varNames;
    }

    public List<String> getActionNames() {
        return actionNames;
    }

    public void setActionNames(List<String> actionNames) {
        this.actionNames = actionNames;
    }

    public List<String> getScriptNames() {
        return scriptNames;
    }

    public void setScriptNames(List<String> scriptNames) {
        this.scriptNames = scriptNames;
    }

    public String getExpressionName() {
        return expressionName;
    }

    public void setExpressionName(String expressionName) {
        this.expressionName = expressionName;
    }

    public String getRuleCode() {
        return ruleCode;
    }

    public void setRuleCode(String ruleCode) {
        this.ruleCode = ruleCode;
    }

    public String getRuleDesc() {
        return ruleDesc;
    }

    public void setRuleDesc(String ruleDesc) {
        this.ruleDesc = ruleDesc;
    }

    public String getRuleTitle() {
        return ruleTitle;
    }

    public void setRuleTitle(String ruleTitle) {
        this.ruleTitle = ruleTitle;
    }

    public Integer getRuleStatus() {
        return ruleStatus;
    }

    public void setRuleStatus(Integer ruleStatus) {
        this.ruleStatus = ruleStatus;
    }

    public JSONObject toOutputJson() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("ruleNameSpace", getNameSpace());
        jsonObject.put("ruleName", getConfigureName());
        if (StringUtil.isNotEmpty(ruleCode)) {
            jsonObject.put("ruleCode", ruleCode);
        }
        if (StringUtil.isNotEmpty(ruleDesc)) {
            jsonObject.put("ruleDesc", ruleDesc);
        }
        if (StringUtil.isNotEmpty(ruleTitle)) {
            jsonObject.put("ruleTitle", ruleTitle);
        }
        if (StringUtil.isNotEmpty(ruleLevel)) {
            jsonObject.put("ruleLevel", ruleLevel);
        }
        return jsonObject;
    }

    public String getRuleLevel() {
        return ruleLevel;
    }

    public void setRuleLevel(String ruleLevel) {
        this.ruleLevel = ruleLevel;
    }
}
