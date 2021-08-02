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
package org.apache.rocketmq.streams.script.service.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.streams.common.cache.softreference.ICache;
import org.apache.rocketmq.streams.common.cache.softreference.impl.SoftReferenceCache;
import org.apache.rocketmq.streams.common.classloader.FileClassLoader;
import org.apache.rocketmq.streams.common.classloader.IsolationClassLoader;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.topology.model.AbstractScript;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.function.service.impl.ScanFunctionService;
import org.apache.rocketmq.streams.script.operator.impl.FunctionScript;
import org.apache.rocketmq.streams.script.service.IScriptService;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class ScriptServiceImpl implements IScriptService {
    /**
     * 所有实现IScriptInit接口的对象和method
     */
    protected ScanFunctionService functionService = ScanFunctionService.getInstance();

    public ScriptServiceImpl() {

    }

    @Override
    public List<IMessage> executeScript(IMessage message, FunctionContext context, AbstractScript<List<IMessage>, FunctionContext> script) {
        script.doMessage(message, context);
        List<IMessage> messages = new ArrayList<>();
        messages.add(message);
        return messages;
    }

    /**
     * 创建软引用缓存，尽量缓存script，减少解析成本，当内存不足时自动回收
     */
    private static ICache<String, FunctionScript> cache =
        new SoftReferenceCache<>(script -> {
            FunctionScript functionScript = new FunctionScript();
            functionScript.setScript(script);
            functionScript.init();
            return functionScript;
        });

    @Override
    public List<IMessage> executeScript(final JSONObject jsonObject, String script) {
        FunctionScript functionScript = cache.get(script);
        return executeScript(jsonObject, functionScript);
    }

    public JSONArray convert(List<IMessage> messages) {
        JSONArray jsonArray = new JSONArray();
        if (messages == null) {
            return jsonArray;
        }
        for (IMessage message : messages) {
            jsonArray.add(message.getMessageBody());
        }
        return jsonArray;
    }

    @Override
    public List<IMessage> executeScript(final JSONObject jsonObject, AbstractScript<List<IMessage>, FunctionContext> script) {
        Message message = new Message(jsonObject);
        FunctionContext context = new FunctionContext(message);
        return executeScript(message, context, script);
    }

    /**
     * @param packageNames
     */

    @Override
    public void scanPackages(String... packageNames) {
        functionService.scanePackages(packageNames);
    }

    @Override
    public void scanDir(String dir, String... packageNames) {
        if (packageNames == null || packageNames.length == 0) {
            return;
        }
        FileClassLoader classLoader = new FileClassLoader(dir, this.getClass().getClassLoader());
        for (String pacakgeName : packageNames) {
            functionService.scanClassDir(dir, pacakgeName, classLoader);
        }
    }

    @Override
    public void scanJar(File jarFile, String packageName) {
        IsolationClassLoader isolationClassLoader =
            new IsolationClassLoader(jarFile.getAbsolutePath(), this.getClass().getClassLoader());
        functionService.scanClassDir(jarFile, packageName, isolationClassLoader);
    }

    private ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(3);

}
