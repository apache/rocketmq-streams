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
package org.apache.rocketmq.streams.script.service.udf;

import com.aliyun.oss.OSSClient;
import com.google.common.collect.Lists;
import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.topology.model.AbstractScript;
import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.ScriptComponent;
import org.apache.rocketmq.streams.script.function.model.FunctionConfigure;
import org.apache.rocketmq.streams.script.function.model.FunctionType;
import org.apache.rocketmq.streams.script.function.service.IFunctionService;
import org.apache.rocketmq.streams.script.service.IScriptUDFInit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 主要是为了兼容外部的udf，或者把任意java的方法发布成函数
 */
public class UDFScript extends AbstractScript implements IScriptUDFInit {

    private final transient ScriptComponent scriptComponent = ScriptComponent.getInstance();

    private static final Logger LOGGER = LoggerFactory.getLogger(UDFScript.class);

    protected transient Object instance;

    protected String fullClassName;//类的全限定名

    protected String methodName;//需要发布成函数的方法名，可以有多个同名的方法

    protected String initMethodName = "open";//如果这个类使用前需要初始化，配置初始化的方法名

    protected String closeMethodName = "close";//如果这个类使用后需要初销毁，配置销毁的方法名

    protected String functionName;//注册的函数名

    protected boolean isURL = false;//这个类存在的位置，如果存在远程，需要http或其他协议下载，这个值设置为true

    /**
     * init method对应的参数
     */
    protected transient Object[] initParameters = null;

    /**
     * 是否完成初始化
     */
    private transient volatile boolean hasInit = false;

    /**
     * Configuable的初始化方法，由框架执行，执行时自动完成注册
     *
     * @return
     */
    @Override protected boolean initConfigurable() {
        registFunctionSerivce(scriptComponent.getFunctionService());
        FunctionConfigure functionConfigure = scriptComponent.getFunctionService().getFunctionConfigure(createInitMethodName(), this.initParameters);
        if (functionConfigure == null) {
            return true;
        }
        if (initParameters != null) {
            functionConfigure.execute(initParameters);
        } else {
            int length = functionConfigure.getParameterDataTypes().length;
            Object[] paras = new Object[length];
            for (int i = 0; i < length; i++) {
                paras[i] = null;
            }
            functionConfigure.execute(paras);
        }
        return true;
    }

    /**
     * 完成函数的注册
     */
    protected void registFunctionSerivce(IFunctionService iFunctionService) {
        if (StringUtil.isEmpty(functionName) || hasInit) {
            return;
        }

        // 将实例化逻辑从initConfigurable抽取到这里的原因：1、实例化过程慢要下载很多JAR影响整体流程。2、将所有pythonudf/blinkjarudf这种需要下载的udf
        // 都统一用单独的定时器处理
        if (initBeanClass(iFunctionService)) {
            Method[] methods = instance.getClass().getMethods();
            for (Method method : methods) {
                if (method.getName().equals(methodName)) {
                    FunctionType functionType = getFunctionType();
                    if (List.class.isAssignableFrom(method.getReturnType())) {
                        iFunctionService.registeUserDefinedUDTFFunction(functionName, instance, method);
                    } else {
                        iFunctionService.registeFunction(functionName, instance, method, functionType);
                    }

                } else if (method.getName().equals(initMethodName)) {
                    iFunctionService.registeFunction(createInitMethodName(), instance, method);
                }
            }

            hasInit = true;
        }
    }

    @Override public void destroy() {
        super.destroy();
        FunctionConfigure functionConfigure = scriptComponent.getFunctionService().getFunctionConfigure(getCloseMethodName());
        if (functionConfigure == null) {
            return;
        }
        int length = functionConfigure.getParameterDataTypes().length;
        Object[] paras = new Object[length];
        for (int i = 0; i < length; i++) {
            paras[i] = null;
        }
        scriptComponent.getFunctionService().directExecuteFunction(getCloseMethodName(), paras);
    }

    /**
     * 在加载时，初始化对象。应该支持，本地class load，从文件load和从远程下载。远程下载部分待测试
     */
    protected boolean initBeanClass(IFunctionService iFunctionService) {

        ClassLoader classLoader = this.getClass().getClassLoader();
        Class<?> clazz;
        try {
            clazz = classLoader.loadClass(fullClassName);
            instance = clazz.newInstance();
        } catch (Exception e) {
            try {
                String jarUrl = getValue();
                URL url = null;
                if (isURL) {
                    url = new URL(getValue());
                } else {
                    if (StringUtil.isNotEmpty(jarUrl)) {
                        if (jarUrl.startsWith("/")) {
                            File file = new File(jarUrl);
                            url = new URL("file", null, file.getCanonicalPath());
                        } else if (jarUrl.startsWith("http://") || jarUrl.startsWith("https://")) {
                            url = new URL(jarUrl);
                        } else if (jarUrl.startsWith("oss://")) {
                            String ossUrl = jarUrl.substring(6); //url以oss://开头
                            String accessKeyId = ComponentCreator.getProperties().getProperty(ComponentCreator.UDF_JAR_OSS_ACCESS_ID);
                            String accesskeySecurity = ComponentCreator.getProperties().getProperty(ComponentCreator.UDF_JAR_OSS_ACCESS_KEY);

                            String[] ossInfo = ossUrl.split("/");
                            String endPoint = ossInfo.length > 0 ? ossInfo[0] : "";
                            String bucketName = ossInfo.length > 1 ? ossInfo[1] : "";
                            List<String> objectNames = ossInfo.length > 2 ? Arrays.asList(ossInfo[2].split(",")) : Lists.newArrayList();

                            OSSClient ossClient = new OSSClient(endPoint, accessKeyId, accesskeySecurity);
                            if (objectNames.size() > 0) {
                                url = ossClient.generatePresignedUrl(bucketName, objectNames.get(0), DateUtils.addMinutes(new Date(), 30));
                            }
                        } else {
                            File file = downLoadFile(jarUrl);
                            url = new URL("file", null, file.getCanonicalPath());
                        }
                    }
                }

                URL[] urls = new URL[] {url};
                classLoader = new URLClassLoader(urls, classLoader);

                clazz = classLoader.loadClass(fullClassName);
                instance = clazz.newInstance();
            } catch (Exception ex) {
                LOGGER.error("加载异常," + ex.getMessage(), ex);
                return false;
            }
        }
        return true;
    }

    protected File downLoadFile(String fileUrl) {
        if (fileUrl.startsWith(FileUtil.LOCAL_FILE_HEADER)) {
            return createFileSupportResourceFile(fileUrl);
        } else if (fileUrl.startsWith(FileUtil.CLASS_PATH_FILE_HEADER)) {
            return createFileSupportResourceFile(fileUrl);
        }
        return null;
    }

    protected static File createFileSupportResourceFile(String fileUrl) {
        if (fileUrl.startsWith(FileUtil.CLASS_PATH_FILE_HEADER)) {
            fileUrl = fileUrl.replaceFirst(FileUtil.CLASS_PATH_FILE_HEADER, "");
            return FileUtil.getResourceFile(fileUrl);
        } else if (fileUrl.startsWith(FileUtil.LOCAL_FILE_HEADER)) {
            fileUrl = fileUrl.replaceFirst(FileUtil.LOCAL_FILE_HEADER, "");
            return new File(fileUrl);
        } else {
            return new File(fileUrl);
        }
    }

    protected String createInitMethodName() {
        return MapKeyUtil.createKey(functionName, initMethodName);
    }

    protected String createCloseMethodName() {
        return MapKeyUtil.createKey(functionName, closeMethodName);
    }

    public String getFullClassName() {
        return fullClassName;
    }

    public void setFullClassName(String fullClassName) {
        this.fullClassName = fullClassName;
    }

    public String getMethodName() {
        return methodName;
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    @Override public String getInitMethodName() {
        return initMethodName;
    }

    public void setInitMethodName(String initMethodName) {
        this.initMethodName = initMethodName;
    }

    protected FunctionType getFunctionType() {
        return FunctionType.UDF;
    }

    @Override public Object getInstance() {
        return instance;
    }

    public void setInstance(Object instance) {
        this.instance = instance;
    }

    public String getFunctionName() {
        return functionName;
    }

    public boolean isURL() {
        return isURL;
    }

    public void setURL(boolean URL) {
        isURL = URL;
    }

    public void setFunctionName(String functionName) {
        this.functionName = functionName;
    }

    @Override public Object doMessage(IMessage channelMessage, AbstractContext context) {
        return instance;
    }

    public String getCloseMethodName() {
        return closeMethodName;
    }

    public void setCloseMethodName(String closeMethodName) {
        this.closeMethodName = closeMethodName;
    }

    @Override public List<String> getScriptsByDependentField(String fieldName) {
        throw new RuntimeException("can not support this method:getScriptsByDependentField");
    }

    @Override public Map<String, List<String>> getDependentFields() {
        return null;
    }
}
