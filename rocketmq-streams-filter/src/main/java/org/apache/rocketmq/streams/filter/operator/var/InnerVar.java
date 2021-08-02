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
package org.apache.rocketmq.streams.filter.operator.var;

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.streams.filter.context.RuleContext;
import org.apache.rocketmq.streams.filter.operator.Rule;

@SuppressWarnings("rawtypes")
public class InnerVar extends Var {

    private static final long serialVersionUID = -166963014761276615L;
    public static final String ORIG_MESSAGE = "inner_message";
    public static final String RULE = "inner_rule";
    public static final String INNER_RULE_CODE = "inner_rule_code";
    public static final String INNER_RULE_DESC = "inner_rule_desc";
    public static final String INNER_RULE_TITEL = "inner_rule_title";
    public static final String INNER_NAMESPACE = "inner_namespace";

    @Override
    public Object doAction(RuleContext context, Rule rule) {
        String varName = getVarName();
        if (varName.equals(ORIG_MESSAGE)) {
            return context.getMessage().getMessageBody().toJSONString();
        } else if (varName.equals(RULE)) {
            return rule.toJson();
        } else if (varName.equals(INNER_RULE_CODE)) {
            return rule.getRuleCode();
        } else if (varName.equals(INNER_NAMESPACE)) {
            return context.getNameSpace();
        } else if (varName.equals(INNER_RULE_TITEL)) {
            return rule.getRuleTitle();
        } else if (varName.equals(INNER_RULE_DESC)) {
            return rule.getRuleDesc();
        }
        return null;
    }

    @Override
    public boolean canLazyLoad() {
        return false;
    }

    @Override
    public boolean volidate(RuleContext context, Rule rule) {
        return true;
    }

    public static void main(String args[]) {

        // String cmdline="\"C:\\Windows\\SysWOW64\\cmd.exe\" /c REG add
        // HKLM\\Software\\Microsoft\\Windows\\CurrentVersion\\Run /v svchost /t REG_SZ /d
        // \"C:\\Windows\\svchost.exe\"";

        String message =
            "{\"uid\":\"N/A\",\"filepath\":\"N/A\",\"sid\":\"-1\",\"ppid\":\"4080\",\"cmdline\":\"REG  add "
                + "HKLM\\\\Software\\\\Microsoft\\\\Windows\\\\CurrentVersion\\\\Run /v svchost /t REG_SZ /d "
                + "\\\"C:\\\\Windows\\\\svchost.exe\\\" /f\",\"egroupid\":\"N/A\",\"groupname\":\"N/A\","
                + "\"cwd\":\"N/A\",\"pid\":\"2144\",\"pcmdline\":\"\\\"C:\\\\Windows\\\\SysWOW64\\\\cmd.exe\\\"  /c "
                + "REG add HKLM\\\\Software\\\\Microsoft\\\\Windows\\\\CurrentVersion\\\\Run /v svchost /t REG_SZ /d "
                + "\\\"C:\\\\Windows\\\\svchost.exe\\\" /f\",\"safe_mode\":\"N/A\",\"username\":\"N/A\","
                + "\"time\":\"2018-02-26 04:18:20\",\"seq\":\"8136606\",\"tty\":\"N/A\",\"filename\":\"\","
                + "\"groupid\":\"N/A\",\"uuid\":\"4c8a102a-52ad-480d-92f3-a9a2cce98773\",\"pfilename\":\"N/A\","
                + "\"messageId\":\"310347\",\"euid\":\"N/A\"}";

        JSONObject obj = JSONObject.parseObject(message);
        //        System.out.println(obj.getString("cmdline"));
        System.out.println(obj.toJSONString());

    }

}
