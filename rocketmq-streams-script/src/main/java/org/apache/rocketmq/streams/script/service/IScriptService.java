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
package org.apache.rocketmq.streams.script.service;

import com.alibaba.fastjson.JSONObject;
import java.io.File;
import java.util.List;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.topology.model.AbstractScript;
import org.apache.rocketmq.streams.script.context.FunctionContext;

/**
 * 脚本服务，脚本是有多个函数构成的。如now=now();nowhh=datefirst(now,'hh');splitByTimeInterval(last,nowhh,'hh',1);from=dateAdd(last,'hh',-1); 脚本执行无返回值,函数会改变message的内容。
 */
public interface IScriptService {

    /**
     * 执行一个脚本
     *
     * @param script 脚本对象
     * @return
     */
    List<IMessage> executeScript(IMessage message, FunctionContext context, AbstractScript<List<IMessage>, FunctionContext> script);

    /**
     * 执行一个脚本
     *
     * @param jsonObject
     * @param script     脚本
     * @return
     */
    List<IMessage> executeScript(final JSONObject jsonObject, String script);

    /**
     * 执行一个脚本
     *
     * @param script
     * @return
     */
    List<IMessage> executeScript(JSONObject message, AbstractScript<List<IMessage>, FunctionContext> script);

    /**
     * 通过扫描包名，发现注册函数，只要类标注了@Function,方法标注了@FunctionMethod就会被注册
     *
     * @param packageNames
     */
    void scanPackages(String... packageNames);

    /**
     * 扫描指定目录，发现注册函数，目录中的class只要类标注了@Function,方法标注了@FunctionMethod就会被注册
     *
     * @param dir
     * @param packageNames
     */
    void scanDir(String dir, String... packageNames);

    /**
     * 扫描一个jar包，发现注册函数，jar中的class只要类标注了@Function,方法标注了@FunctionMethod就会被注册
     *
     * @param jarFile
     * @param packageName
     */
    void scanJar(File jarFile, String packageName);

}
