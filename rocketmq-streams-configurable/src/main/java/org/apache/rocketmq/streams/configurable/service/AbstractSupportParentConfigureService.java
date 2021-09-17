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
package org.apache.rocketmq.streams.configurable.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.utils.StringUtil;

/**
 * namespace 分层，支持顶级命名空间，顶级命名空间的对象，所有命名空间都可见。顶级命名空间是固定值IConfigurableService.PARENT_CHANNEL_NAME_SPACE
 */
public abstract class AbstractSupportParentConfigureService extends AbstractConfigurableService
    implements IConfigurableService {

    private static final Log LOG = LogFactory.getLog(AbstractSupportParentConfigureService.class);
    protected IConfigurableService configureService = null;
    protected IConfigurableService parentConfigureService = null;
    //protected IConfigurableService shareConfigureService = null;
    protected Properties properties;

    public AbstractSupportParentConfigureService() {
        super(null);
    }

    public void initMethod(Properties property) {
        this.properties = property;
        initBeforeInitConfigurable(property);
    }

    protected abstract void initBeforeInitConfigurable(Properties property);

    @Override
    public void initConfigurables(String namespace) {

        if (!IConfigurableService.PARENT_CHANNEL_NAME_SPACE.equals(namespace)) {
            parentConfigureService.initConfigurables(IConfigurableService.PARENT_CHANNEL_NAME_SPACE);
        } else {
            parentConfigureService = null;
        }
        configureService.initConfigurables(namespace);
    }

    @Override
    public boolean refreshConfigurable(String namespace) {

        if (!IConfigurableService.PARENT_CHANNEL_NAME_SPACE.equals(namespace)) {
            parentConfigureService.refreshConfigurable(IConfigurableService.PARENT_CHANNEL_NAME_SPACE);
            // initShareConfigurableService(namespace);
        }
        configureService.refreshConfigurable(namespace);
        return true;
    }

    @Override
    public List<IConfigurable> queryConfigurable(String type) {
        List<IConfigurable> result = configureService.queryConfigurable(type);
        if (result == null) {
            result = new ArrayList<>();
        }
        //if (shareConfigureService != null) {
        //    List<IConfigurable> share = shareConfigureService.queryConfigurable(type);
        //    if (share != null) {
        //        result.addAll(share);
        //    }
        //}
        if (parentConfigureService == null) {
            return result;
        }
        List<IConfigurable> parent = parentConfigureService.queryConfigurable(type);
        if (parent != null) {
            result.addAll(parent);
        }
        return result;
    }

    @Override
    public IConfigurable queryConfigurableByIdent(String type, String name) {
        IConfigurable configurable = configureService.queryConfigurableByIdent(type, name);
        if (configurable != null) {
            return configurable;
        }
        if (parentConfigureService == null) {
            return null;
        }
        //if (shareConfigureService != null) {
        //    configurable = shareConfigureService.queryConfigurableByIdent(type, name);
        //}
        if (configurable != null) {
            return configurable;
        }
        return parentConfigureService.queryConfigurableByIdent(type, name);
    }

    @Override
    public IConfigurable queryConfigurableByIdent(String identification) {
        IConfigurable configurable = configureService.queryConfigurableByIdent(identification);
        if (configurable != null) {
            return configurable;
        }
        if (parentConfigureService == null) {
            return null;
        }
        //if (shareConfigureService != null) {
        //    configurable = shareConfigureService.queryConfigurableByIdent(identification);
        //}
        if (configurable != null) {
            return configurable;
        }
        return parentConfigureService.queryConfigurableByIdent(identification);
    }

    @Override
    protected void insertConfigurable(IConfigurable configurable) {
        if (parentConfigureService != null && configurable.getNameSpace()
            .equals(IConfigurableService.PARENT_CHANNEL_NAME_SPACE)) {
            parentConfigureService.insert(configurable);
        } else {
            configureService.insert(configurable);
        }
    }

    @Override
    protected void updateConfigurable(IConfigurable configurable) {
        if (parentConfigureService != null && configurable.getNameSpace()
            .equals(IConfigurableService.PARENT_CHANNEL_NAME_SPACE)) {
            parentConfigureService.update(configurable);
        } else {
            configureService.update(configurable);
        }
    }

    @Override
    public <T> T queryConfigurable(String configurableType, String name) {
        return (T)queryConfigurableByIdent(configurableType, name);
    }

    @Override
    protected GetConfigureResult loadConfigurable(String namespace) {
        return null;
    }

    //protected void initShareConfigurableService(String namespace) {
    //    if (parentConfigureService == null) {
    //        return;
    //    }
    //    shareConfigureService = new AbstractReadOnlyConfigurableService() {
    //
    //        @Override
    //        public <T extends IConfigurable> List<T> loadConfigurableFromStorage(String type) {
    //            refreshConfigurable(namespace);
    //            return queryConfigurableByType(type);
    //        }
    //
    //        @Override
    //        protected List<IConfigurable> loadConfigurables(String namespace) {
    //            List<IConfigurable> parent = parentConfigureService.queryConfigurable(ShareConfiguable.TYPE);
    //            List<IConfigurable> shareConfigurables = new ArrayList<>();
    //            if (parent == null) {
    //                return shareConfigurables;
    //            }
    //            for (IConfigurable configurable : parent) {
    //                ShareConfiguable shareConfiguable = (ShareConfiguable) configurable;
    //                if (shareConfiguable.getShareAll() || shareConfiguable.getShareNameSpaces().contains(namespace)) {
    //                    String sharedNameSpace = shareConfiguable.getSharedNameSpace();
    //                    String sharedType = shareConfiguable.getSharedType();
    //                    String sharedName = shareConfiguable.getSharedName();
    //                    List<IConfigurable> sharedConfigrables =
    //                        createAndQueryConfigurable(sharedNameSpace, sharedType, sharedName);
    //                    if (sharedConfigrables != null) {
    //                        shareConfigurables.addAll(sharedConfigrables);
    //                    }
    //                }
    //            }
    //            return shareConfigurables;
    //        }
    //
    //
    //    };
    //    shareConfigureService.refreshConfigurable(namespace);
    //
    //}

    protected List<IConfigurable> createAndQueryConfigurable(String sharedNameSpace, String sharedType,
                                                             String sharedName) {
        IConfigurableService innerSharedConfigurableService =
            ConfigurableServiceFactory.createConfigurableService(properties);
        innerSharedConfigurableService.refreshConfigurable(sharedNameSpace);
        if (StringUtil.isNotEmpty(sharedName)) {
            List<IConfigurable> configurables = new ArrayList<>();
            IConfigurable configurable = innerSharedConfigurableService.queryConfigurableByIdent(sharedType, sharedName);
            configurables.add(configurable);
            return configurables;
        } else {
            return innerSharedConfigurableService.queryConfigurable(sharedType);
        }

    }

    @Override
    public Collection<IConfigurable> findAll() {
        List<IConfigurable> configurables = new ArrayList<>();
        if (parentConfigureService != null) {
            Collection<IConfigurable> tmp = parentConfigureService.findAll();
            if (tmp != null || tmp.size() > 0) {
                configurables.addAll(tmp);
            }
        }
        Collection<IConfigurable> tmp = configureService.findAll();
        if (tmp != null || tmp.size() > 0) {
            configurables.addAll(tmp);
        }
        return configurables;
    }

    public IConfigurableService getConfigureService() {
        return configureService;
    }

    @Override
    public <T extends IConfigurable> List<T> loadConfigurableFromStorage(String type) {
        List<T> configurables = new ArrayList<>();
        if (parentConfigureService != null) {
            Collection<T> tmp = parentConfigureService.loadConfigurableFromStorage(type);
            if (tmp != null || tmp.size() > 0) {
                configurables.addAll(tmp);
            }
        }
        Collection<T> tmp = configureService.loadConfigurableFromStorage(type);
        if (tmp != null || tmp.size() > 0) {
            configurables.addAll(tmp);
        }
        return configurables;
    }
}
