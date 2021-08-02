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
package org.apache.rocketmq.streams.filter.contants;

/**
 * 规则运行状态
 */
public enum RuleStatus {
    RULE_STATUS_TESTING(0, "测试中", "yundun-sasruleengine-CSZ-key-001"),
    RULE_STATUS_OBSERVING(1, "观察中", "yundun-sasruleengine-GCZ-key-002"),
    RULE_STATUS_AUDITING(2, "审核中", "yundun-sasruleengine-SHZ-key-003"),
    RULE_STATUS_ONLINE(3, "已上线", "yundun-sasruleengine-YSX-key-004"),
    RULE_STATUS_SELF(4, "自定义", "do-self"),
    KILLCHAIN_DESC_RULES(5, "killchain说明规则", "killchain-desc");

    private Integer ruleStatus;
    private String desc;
    private String key;

    private RuleStatus(int ruleStatus, String desc, String key) {
        this.ruleStatus = ruleStatus;
        this.desc = desc;
        this.key = key;
    }

    public Integer getRuleStatus() {
        return ruleStatus;
    }

    public void setRuleStatus(Integer ruleStatus) {
        this.ruleStatus = ruleStatus;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public static String getDescByRuleStatus(Integer ruleStatus) {
        if (RuleStatus.RULE_STATUS_OBSERVING.getRuleStatus().equals(ruleStatus)) {
            return RuleStatus.RULE_STATUS_OBSERVING.getDesc();
        } else if (RuleStatus.RULE_STATUS_AUDITING.getRuleStatus().equals(ruleStatus)) {
            return RuleStatus.RULE_STATUS_AUDITING.getDesc();
        } else if (RuleStatus.RULE_STATUS_ONLINE.getRuleStatus().equals(ruleStatus)) {
            return RuleStatus.RULE_STATUS_ONLINE.getDesc();
        } else {
            return RuleStatus.RULE_STATUS_TESTING.getDesc();
        }
    }

    public static String getKeyByRuleStatus(Integer ruleStatus) {
        if (RuleStatus.RULE_STATUS_OBSERVING.getRuleStatus().equals(ruleStatus)) {
            return RuleStatus.RULE_STATUS_OBSERVING.getKey();
        } else if (RuleStatus.RULE_STATUS_AUDITING.getRuleStatus().equals(ruleStatus)) {
            return RuleStatus.RULE_STATUS_AUDITING.getKey();
        } else if (RuleStatus.RULE_STATUS_ONLINE.getRuleStatus().equals(ruleStatus)) {
            return RuleStatus.RULE_STATUS_ONLINE.getKey();
        } else {
            return RuleStatus.RULE_STATUS_TESTING.getKey();
        }
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public static void main(String args[]) {
        // System.out.println(SasRuleEnum.getDescByRuleStatus(new Integer(1)));
        System.out.println(RuleStatus.RULE_STATUS_OBSERVING.getRuleStatus());

        Integer a = new Integer(1);
        int b = 1;
        System.out.println(a == b);

    }

}
