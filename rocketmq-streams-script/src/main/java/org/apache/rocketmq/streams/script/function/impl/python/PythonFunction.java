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
package org.apache.rocketmq.streams.script.function.impl.python;

import com.alibaba.fastjson.JSONObject;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.streams.common.cache.softreference.ICache;
import org.apache.rocketmq.streams.common.cache.softreference.impl.SoftReferenceCache;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configuration.SystemContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.operator.impl.JPythonScriptOperator;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class PythonFunction {
    protected static AtomicInteger PROC_COUNT = new AtomicInteger(5);
    private ICache<String, JPythonScriptOperator> cache = new SoftReferenceCache<>();

    public JSONObject doPython(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "python名称") String scriptValue) {
        scriptValue = FunctionUtils.getValueString(message, context, scriptValue);
        JPythonScriptOperator pythonScriptOperator = cache.get(scriptValue);
        if (pythonScriptOperator == null) {
            pythonScriptOperator = new JPythonScriptOperator();
            pythonScriptOperator.setValue(scriptValue);
            pythonScriptOperator.init();
            cache.put(scriptValue, pythonScriptOperator);
        }

        pythonScriptOperator.doMessage(message, context);
        return message.getMessageBody();
    }

    @FunctionMethod(value = "py", alias = "python", comment = "执行一个指定名称的python脚本")
    public Object pythonUDF(String fileName, String... args) {

        Process proc;
        BufferedReader in = null;

        try {

            while (PROC_COUNT.decrementAndGet() < 0) {
                synchronized (this) {
                    PROC_COUNT.incrementAndGet();
                    this.notify();
                    if (PROC_COUNT.get() > 0) {
                        break;
                    } else {
                        this.wait();
                    }
                }
            }
            proc = Runtime.getRuntime().exec("python " + createFilePath(fileName) + " " + MapKeyUtil.createKeyBySign(" ", args));// 执行py文件
            //用输入输出流来截取结果
            in = new BufferedReader(new InputStreamReader(proc.getInputStream()));
            StringBuilder result = new StringBuilder();
            String line = null;
            while ((line = in.readLine()) != null) {
                result.append(line);
            }
            proc.waitFor();
            return result;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            try {
                in.close();
                synchronized (this) {
                    PROC_COUNT.incrementAndGet();
                    this.wait();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    private String createFilePath(String fileName) {
        return FileUtil.concatFilePath(SystemContext.getProperty(ComponentCreator.UDF_JAR_PATH), fileName);
    }
}
