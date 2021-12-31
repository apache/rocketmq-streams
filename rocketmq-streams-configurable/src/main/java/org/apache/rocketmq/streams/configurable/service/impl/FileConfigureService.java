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
package org.apache.rocketmq.streams.configurable.service.impl;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.configure.ConfigureFileKey;
import org.apache.rocketmq.streams.common.model.Entity;
import org.apache.rocketmq.streams.common.utils.AESUtil;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.configurable.model.Configure;
import org.apache.rocketmq.streams.configurable.service.AbstractConfigurableService;

public class FileConfigureService extends AbstractConfigurableService {

    public static final String FILE_PATH_NAME = IConfigurableService.FILE_PATH_NAME;
    // 配置文件的路径
    private static final Log LOG = LogFactory.getLog(FileConfigureService.class);
    private static final String DEFAULT_FILE_NAME = "dipper.cs";                        // 默认文件名
    private static final String SIGN = "&&&&";                                       // 字段分割附号
    public String fileName;

    public FileConfigureService(Properties properties) {
        super(properties);
        initService(properties.getProperty(FILE_PATH_NAME));
    }

    protected void initService(String fileAndPath) {
        if (StringUtil.isEmpty(fileAndPath)) {
            String path = this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
            if (path.endsWith(".jar")) {
                int index = path.lastIndexOf(File.separator);
                path = path.substring(0, index);
            }
            fileName = FileUtil.concatFilePath(path, DEFAULT_FILE_NAME);
        } else {
            fileName = fileAndPath;
        }
        LOG.info("load file from path = " + fileName);
    }

    @Override
    protected GetConfigureResult loadConfigurable(String namespace) {
        GetConfigureResult result = new GetConfigureResult();
        try {
            List<Configure> configures = selectOpening(namespace);
            List<IConfigurable> configurables = convert(configures);
            LOG.info("load configure namespace=" + namespace + " count=" + configures.size());
            result.setConfigurables(configurables);
            // 该字段标示查询是否成功，若不成功则不会更新配置
            result.setQuerySuccess(true);
        } catch (Exception e) {
            result.setQuerySuccess(false);
            e.printStackTrace();
            LOG.error("load configurable error ", e);
        }
        return result;
    }

    protected List<Configure> selectOpening(String namespace) {
        List<String> list = loadFileLine(fileName);
        List<Configure> configures = convert2Configure(list);
        return filter(configures, namespace);
    }

    protected List<Configure> filter(List<Configure> configures, String namespace) {
        if (configures == null) {
            return new ArrayList<>();
        }
        if (StringUtil.isEmpty(namespace)) {
            throw new RuntimeException("namespace can not empty ");
        }
        List<Configure> filterConfigures = new ArrayList<>();
        for (Configure configure : configures) {
            if (!namespace.equals(configure.getNameSpace())) {
                continue;
            }
            filterConfigures.add(configure);
        }
        return filterConfigures;
    }

    @Override
    protected void insertConfigurable(IConfigurable configure) {
        if (configure == null) {
            LOG.warn("insert configure is null");
            return;
        }
        String row = configure2String(configure);

        List<String> rows = loadFileLine(fileName);
        if (rows == null) {
            rows = new ArrayList<>();
        }
        List<Configure> configures = convert2Configure(rows);
        String newKey =
            MapKeyUtil.createKey(configure.getNameSpace(), configure.getType(), configure.getConfigureName());
        boolean isReplace = false;
        for (int i = 0; i < configures.size(); i++) {
            Configure c = configures.get(i);
            String old = MapKeyUtil.createKey(c.getNameSpace(), c.getType(), c.getName());
            if (old.equals(newKey)) {
                rows.set(i, configure2String(configure));
                isReplace = true;
                break;
            }
        }
        if (!isReplace) {
            rows.add(configure2String(configure));
        }
        writeFile(fileName, rows);
    }

    @Override
    protected void updateConfigurable(IConfigurable configure) {
        if (configure == null) {
            LOG.warn("insert configure is null");
            return;
        }

        List<String> rows = FileUtil.loadFileLine(fileName);
        if (rows == null) {
            rows = new ArrayList<>();
        }
        for (int i = 0; i < rows.size(); i++) {
            String row = rows.get(i);
            Configure oldConfigure = convert(row);
            if (configure.getNameSpace().equals(oldConfigure.getNameSpace()) && configure.getType()
                .equals(oldConfigure.getType()) && configure.getConfigureName().equals(oldConfigure.getName())) {
                rows.set(i, configure2String(configure));
            }
        }
        writeFile(fileName, rows);

    }

    protected Configure convert(String row) {
        String[] values = row.split(SIGN);
        String namespace = getColumnValue(values, 0, "namespace");
        String type = getColumnValue(values, 1, "type");
        String name = getColumnValue(values, 2, "name");
        String jsonValue = getColumnValue(values, 3, "json_value");
        try {
            jsonValue = AESUtil.aesDecrypt(jsonValue, ComponentCreator.getProperties().getProperty(ConfigureFileKey.SECRECY, ConfigureFileKey.SECRECY_DEFAULT));
        } catch (Exception e) {
            LOG.error("failed in decrypting the value, reason:\t" + e.getCause());
            throw new RuntimeException(e);
        }
        String createDate = getColumnValue(values, 4, "gmt_create");
        String modifiedDate = getColumnValue(values, 5, "gmt_modified");
        String id = getColumnValue(values, 6, "id");
        Configure configure = new Configure();
        configure.setNameSpace(namespace);
        configure.setType(type);
        configure.setName(name);
        configure.setJsonValue(jsonValue);
        configure.setGmtCreate(DateUtil.parse(createDate));
        configure.setGmtCreate(DateUtil.parse(modifiedDate));
        configure.setId((id == null ? null : Long.valueOf(id)));

        return configure;
    }

    protected List<Configure> convert2Configure(List<String> rows) {
        List<Configure> configures = new ArrayList<Configure>();
        for (String row : rows) {
            configures.add(convert(row));
        }
        return configures;
    }

    protected String getColumnValue(String[] values, int i, String namespace) {
        if (values == null || values.length == 0) {
            return null;
        }
        if (values.length <= i) {
            return null;
        }
        if ("null".equals(values[i])) {
            return null;
        }
        return values[i];
    }

    /**
     * 解密文件，并加载到内存
     *
     * @param fileName
     * @return
     */
    protected List<String> loadFileLine(String fileName) {
        List<String> rows = FileUtil.loadFileLine(fileName);
        if (rows == null) {
            rows = new ArrayList<>();
        }
        return doDecRowList(rows);
    }

    protected void writeFile(String fileName, List<String> rows) {
        List<String> rowList = doEncryptRowList(rows);
        FileUtil.write(fileName, rowList);
    }

    private List<String> doEncryptRowList(List<String> rows) {
        return rows;
    }

    private List<String> doDecRowList(List<String> rows) {
        return rows;
    }

    protected String configure2String(IConfigurable configure) {
        Entity entity = null;
        if (configure instanceof Entity) {
            entity = (Entity) configure;
        } else {
            entity = new Entity();
        }
        String theSecretValue = null;
        try {
            theSecretValue = AESUtil.aesEncrypt(configure.toJson(), ComponentCreator.getProperties().getProperty(ConfigureFileKey.SECRECY, ConfigureFileKey.SECRECY_DEFAULT));
        } catch (Exception e) {
            LOG.error("failed in encrypting the value, reason:\t" + e.getCause());
            throw new RuntimeException(e);
        }
        String row = MapKeyUtil.createKeyBySign(SIGN, configure.getNameSpace(), configure.getType(),
            configure.getConfigureName(), theSecretValue, DateUtil.format(entity.getGmtCreate()),
            DateUtil.format(entity.getGmtModified()), entity.getId() + "");
        return row;
    }

    public String getFileName() {
        return fileName;
    }

    @Override
    public <T extends IConfigurable> List<T> loadConfigurableFromStorage(String type) {
        refreshConfigurable(getNamespace());
        return queryConfigurableByType(type);
    }

}
