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
package org.apache.rocketmq.streams.filter.engine;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONObject;

public class SplitFlow {

    private static final Log LOG = LogFactory.getLog(SplitFlow.class);
    private volatile String nameSpace;
    private JSONObject uuidNameMap = new JSONObject();

    // uuid尾号
    private volatile String uuidEndNum = NUMBERS_ALL;
    private volatile String ageis_split_namespaces = "topic_aegis_detect_msg_proc";
    private volatile String sas_split_namespaces;

    // 全部尾号
    private static final String NUMBERS_ALL = "all";
    // 没有尾号
    private static final String NUMBERS_NONE = "none";

    private void paserConfigToField(String jsonValue) {
        JSONObject jsonObject = JSONObject.parseObject(jsonValue);
        this.uuidEndNum = jsonObject.getString("uuidEndNum");
        this.ageis_split_namespaces = jsonObject.getString("ageis_split_namespaces");
        this.sas_split_namespaces = jsonObject.getString("sas_split_namespaces");
    }

    /**
     * 验证安骑士数据是否满足分流后的条件
     *
     * @return
     */
    public boolean aegisUuidMatch(String uuid) {
        if (uuidEndNum == null || "".equals(uuidEndNum)) {
            LOG.warn("SplitFlow aegisUuidMatch,uuidEndNum is null");
            return true;
        }
        String numbers = uuidEndNum;

        if (NUMBERS_ALL.equals(numbers)) {
            return true;
        } else if (NUMBERS_NONE.equals(numbers)) {
            return false;
        } else {
            return this.getNumberResult(uuid, numbers);
        }
    }

    /**
     * 验证安骑士数据是否满足分流后的条件
     *
     * @param message
     * @return
     */
    public boolean sasMatch(JSONObject message) {
        return true;
    }

    public String getUuidNameByNameSpace(String nameSpace) {
        String uuid = uuidNameMap.getString(nameSpace);
        if (uuid == null) {
            uuid = "uuid";
        }
        return uuid;
    }

    public void setUuidNameJson(String uuidNameJson) {
        try {
            this.uuidNameMap = JSONObject.parseObject(uuidNameJson);
        } catch (Exception e) {
            LOG.error("SplitFlow setUuidNameJson error" + uuidNameJson, e);
        }

    }

    /**
     * 判断尾号是否满足条件
     *
     * @return
     */
    private boolean getNumberResult(String aliuid, String userNumbers) {
        String[] numbers = userNumbers.split(",");
        for (String number : numbers) {
            if (aliuid.endsWith(number)) {
                return true;
            }
        }
        return false;
    }

    public String getNameSpace() {
        return nameSpace;
    }

    public void setNameSpace(String nameSpace) {
        this.nameSpace = nameSpace;
    }

    public String getAgeis_split_namespaces() {
        return ageis_split_namespaces;
    }

    public void setAgeis_split_namespaces(String ageis_split_namespaces) {
        this.ageis_split_namespaces = ageis_split_namespaces;
    }

    public String getSas_split_namespaces() {
        return sas_split_namespaces;
    }

    public void setSas_split_namespaces(String sas_split_namespaces) {
        this.sas_split_namespaces = sas_split_namespaces;
    }

    public void setSplitFlowJson(String splitFlowJson) {
        try {
            paserConfigToField(splitFlowJson);
        } catch (Exception e) {
            LOG.error("SplitFlow setSplitFlowJson error,splitFlowJson is:" + splitFlowJson, e);
        }

    }

    public JSONObject getUuidNameMap() {
        return uuidNameMap;
    }

    public void setUuidNameMap(JSONObject uuidNameMap) {
        this.uuidNameMap = uuidNameMap;
    }

    public String getUuidEndNum() {
        return uuidEndNum;
    }

    public void setUuidEndNum(String uuidEndNum) {
        this.uuidEndNum = uuidEndNum;
    }

}
