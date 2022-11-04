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

import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configurable.annotation.Changeable;
import org.apache.rocketmq.streams.common.configure.ConfigureFileKey;
import org.apache.rocketmq.streams.common.model.Entity;
import org.apache.rocketmq.streams.common.utils.AESUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractConfigurable extends Entity implements IConfigurable {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractConfigurable.class);

    /**
     * 升级中心表
     */
    public static final String TABLE_NAME = "dipper_configure";

    @Changeable protected transient Map<String, Object> privateDatas = new HashMap<>();

    protected transient IConfigurableService configurableService;

    /**
     * 是否完成初始化
     */
    private transient boolean hasInit = false;

    /**
     * 是否初始化成功
     */
    protected transient boolean initSuccess = true;

    /**
     * 是否已经被销毁
     */
    protected transient boolean isDestroy = false;

    /**
     * 数据库的状态字段
     */
    private static final String STATUS = "status";

    @Override public boolean init() {
        boolean initConfigurable = true;
        if (!hasInit) {
            try {
                privateDatas = new HashMap<>();
                initSuccess = true;
                isDestroy = false;
                initConfigurable = initConfigurable();
                initSuccess = initConfigurable;
            } catch (Exception e) {
                initSuccess = false;
                LOGGER.error("[{}] init configurable error, {}", getConfigureName(), this.toJson(), e);
                throw new RuntimeException("init configurable error " + this.toJson(), e);
            }
            hasInit = true;
        }
        return initConfigurable;
    }

    @Override public void destroy() {
        isDestroy = true;
    }

    /**
     * 启用configurable 对象，可以被看到和应用
     */
    public void open() {
        putPrivateData(STATUS, "1");
    }

    /**
     * 关闭configuable 对象，对象失效
     */
    public void close() {
        putPrivateData(STATUS, "0");
    }

    protected boolean initConfigurable() {
        return true;
    }

    public String createSQL() {
        return createSQL(this, TABLE_NAME);
    }

    public static String createSQL(IConfigurable configurable) {
        return createSQL(configurable, "dipper_configure");
    }

    public static String createSQL(IConfigurable configurable, String tableName) {
        String json = configurable.toJson();
        int status = 1;
        if (configurable.getPrivateData("status") != null) {
            status = Integer.parseInt(configurable.getPrivateData("status"));
        }
        String theSecretValue;
        try {
            theSecretValue = AESUtil.aesEncrypt(json, ComponentCreator.getProperties().getProperty(ConfigureFileKey.SECRECY, ConfigureFileKey.SECRECY_DEFAULT));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return "insert into " + tableName + "(gmt_create,gmt_modified,namespace,type,name,json_value,status)" + "values(" + "now(),now(),'" + configurable.getNameSpace() + "','" + configurable.getType() + "','" + configurable.getConfigureName() + "','" + theSecretValue + "'," + status + ")" + "ON DUPLICATE KEY UPDATE status=" + status + ", gmt_modified = now()" + ",json_value='" + theSecretValue + "'";
    }

    public void update() {
        if (configurableService != null) {
            configurableService.update(this);
        } else {
            LOGGER.debug("[{}] can not support configurable update configurable service is null", getConfigureName());
        }
    }

    protected String getDipperConfigureTableName() {
        return TABLE_NAME;
    }

    public <T extends IConfigurableService> T getConfigurableService() {
        return (T) configurableService;
    }

    public void setConfigurableService(IConfigurableService configurableService) {
        this.configurableService = configurableService;
    }

    @Override public <T> void putPrivateData(String key, T value) {
        this.privateDatas.put(key, value);
    }

    @Override public <T> T getPrivateData(String key) {
        return (T) this.privateDatas.get(key);
    }

    public <T> T removePrivateData(String key) {
        return (T) this.privateDatas.remove(key);
    }

    @Override public Map<String, Object> getPrivateData() {
        return this.privateDatas;
    }

    public void setPrivateDatas(Map<String, Object> privateDatas) {
        this.privateDatas = privateDatas;
    }

    public boolean isInitSuccess() {
        return initSuccess;
    }

    public void setInitSuccess(boolean initSuccess) {
        this.initSuccess = initSuccess;
    }

    public boolean isDestroy() {
        return isDestroy;
    }

    public boolean isHasInit() {
        return hasInit;
    }

    public void setHasInit(boolean hasInit) {
        this.hasInit = hasInit;
    }

}
