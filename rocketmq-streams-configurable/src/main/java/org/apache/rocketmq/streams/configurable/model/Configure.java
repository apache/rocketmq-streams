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
package org.apache.rocketmq.streams.configurable.model;

import org.apache.rocketmq.streams.common.model.Entity;

/**
 * configuable如果存储在db，这个是db表的映射对象
 */
public class Configure extends Entity {

    private static final long serialVersionUID = 5668017348345235669L;

    private String nameSpace;
    private String type;
    private String name;
    // private String identification;
    private String jsonValue;
    private String modifyTime;
    private String remark;
    private int openRange;

    public static String createTableSQL(String tableName) {
        return "/******************************************/\n"
            + "/*   TableName = dipper_configure   */\n"
            + "/******************************************/\n"
            + "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',\n"
            + "  `gmt_create` datetime NOT NULL COMMENT '创建时间',\n"
            + "  `gmt_modified` datetime NOT NULL COMMENT '修改时间',\n"
            + "  `namespace` varchar(32) NOT NULL COMMENT '项目标识',\n"
            + "  `type` varchar(32) NOT NULL COMMENT '配置类型',\n"
            + "  `name` varchar(128) NOT NULL COMMENT '配置名称',\n"
            + "  `json_value` text NOT NULL COMMENT '配置内容',\n"
            + "  `status` tinyint(3) unsigned NOT NULL DEFAULT '0' COMMENT '1:正在使用 0:已失效',\n"
            + "  PRIMARY KEY (`id`),\n"
            + "  UNIQUE KEY `uk_namespace_type_name` (`namespace`,`type`,`name`),\n"
            + "  KEY `idx_namespace` (`namespace`)\n"
            + ") ENGINE=InnoDB AUTO_INCREMENT=1814834 DEFAULT CHARSET=utf8 COMMENT='统一接入配置项'\n"
            + ";";
    }

    public String getNameSpace() {
        return nameSpace;
    }

    public void setNameSpace(String nameSpace) {
        this.nameSpace = nameSpace;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    // public String getIdentification() {
    // return identification;
    // }

    // public void createIdentification() {
    // this.identification = MapKeyUtil.createKey(nameSpace, type, name);
    // }

    public String getJsonValue() {
        return jsonValue;
    }

    public void setJsonValue(String jsonValue) {
        this.jsonValue = jsonValue;
    }

    public String getModifyTime() {
        return modifyTime;
    }

    public void setModifyTime(String modifyTime) {
        this.modifyTime = modifyTime;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public int getOpenRange() {
        return openRange;
    }

    public void setOpenRange(int openRange) {
        this.openRange = openRange;
    }

    @Override
    public String toString() {
        return "Configure{" + "nameSpace='" + nameSpace + '\'' + ", type='" + type + '\'' + ", name='" + name + '\''
            + ", jsonValue='" + jsonValue + '\'' + ", modifyTime='" + modifyTime + '\'' + ", remark='" + remark + '\''
            + ", openRange=" + openRange + '}';
    }
}
