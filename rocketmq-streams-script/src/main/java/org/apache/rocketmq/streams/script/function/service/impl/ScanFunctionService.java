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
package org.apache.rocketmq.streams.script.function.service.impl;

import java.io.File;
import java.lang.reflect.Modifier;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.calssscaner.AbstractScan;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.function.service.IFunctionService;

/**
 * 提供扫描function的能力
 */
public class ScanFunctionService extends DefaultFunctionServiceImpl implements IFunctionService {

    private static final Log LOG = LogFactory.getLog(ScanFunctionService.class);

    private static final ScanFunctionService functionService = new ScanFunctionService();

    protected AbstractScan scan = new AbstractScan() {
        @Override
        protected void doProcessor(Class clazz) {
            try {
                if (clazz.isInterface() || Modifier.isAbstract(clazz.getModifiers()) || Modifier.isTransient(
                    clazz.getModifiers()) || Modifier.isPrivate(clazz.getModifiers())) {
                    return;
                }
                if (clazz.getName().indexOf("$") != -1) {
                    return;
                }
                Object o = clazz.newInstance();
                Function annotation = o.getClass().getAnnotation(Function.class);
                if (annotation == null) {
                    return;
                }
                registeFunction(o);

            } catch (Exception e) {
                e.printStackTrace();
                LOG.error("初始化类错误" + e.getMessage(), e);
            }
        }
    };

    public ScanFunctionService() {
        this(true);
    }

    public ScanFunctionService(boolean scanDipper) {
        super();
        if (scanDipper) {
            scan.scanPackage("org.apache.rocketmq.streams.script.function.impl");
            scan.scanPackage("org.apache.rocketmq.streams.filter.function");
            scan.scanPackage("org.apache.rocketmq.streams.dim.function");
        }
    }

    public static ScanFunctionService getInstance() {
        return functionService;
    }

    public void scanClassDir(File jarFile, String packageName, ClassLoader classLoader) {
        scan.scanClassDir(jarFile, packageName, classLoader);
    }

    public void scanClassDir(String dir, String packageName, ClassLoader classLoader) {
        scan.scanClassDir(dir, packageName, classLoader);
    }

    public void scanePackages(String... packageNames) {
        scan.scanPackages(packageNames);
    }

    public void scanePackage(String packageName) {
        scan.scanPackages(packageName);

    }

}
