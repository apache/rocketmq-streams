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
package org.apache.rocketmq.streams.script;

import java.util.Properties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.component.AbstractComponent;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.component.IgnoreNameSpace;
import org.apache.rocketmq.streams.script.function.service.impl.ScanFunctionService;
import org.apache.rocketmq.streams.script.service.IScriptService;
import org.apache.rocketmq.streams.script.service.impl.ScriptServiceImpl;

/**
 * 执行脚本，多个函数形成脚本。也可以通过脚本控制流的运转 可以自动扫描 标注了@Function的类。通过scan 来指定扫描的包路径
 */
public class ScriptComponent extends AbstractComponent<IScriptService> implements IgnoreNameSpace {

    private static final ScriptComponent scriptComponent = ComponentCreator.getComponent(null, ScriptComponent.class);
    /**
     * 所有实现IScriptInit接口的对象和method
     */
    protected ScanFunctionService functionService = ScanFunctionService.getInstance();

    protected IScriptService scriptService;

    public static ScriptComponent getInstance() {
        return scriptComponent;
    }

    @Override
    public boolean startComponent(String namespace) {
//        functionService.scanPackage("org.apache.rocketmq.streams.script.function.impl");
//        functionService.scanPackage("org.apache.rocketmq.streams.dim.function.script");
        return true;
    }

    @Override
    public boolean stop() {
        return true;
    }

    @Override
    public IScriptService getService() {
        return scriptService;
    }

    @Override
    protected boolean initProperties(Properties properties) {
        scriptService = new ScriptServiceImpl();
        return true;
    }

    public ScanFunctionService getFunctionService() {
        return functionService;
    }

}
