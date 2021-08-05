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
package org.apache.rocketmq.streams.common.calssscaner;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;

public abstract class AbstractScan {

    private static final Log LOG = LogFactory.getLog(AbstractScan.class);

    private static final String CLASS_REAR = ".class";

    protected Set<String> scanDirs = new HashSet<>();

    public void scanClassDir(File jarFile, String packageName, ClassLoader classLoader) {
        scanClassInJar(jarFile.getAbsolutePath(), packageName, classLoader);
    }

    public void scanClassDir(String dir, String packageName, ClassLoader classLoader) {
        if (StringUtil.isEmpty(dir)) {
            return;
        }
        if (classLoader == null) {
            classLoader = this.getClass().getClassLoader();
        }
        if (!dir.endsWith(File.separator)) {
            dir += File.separator;
        }
        if (packageName.contains(".")) {
            dir += packageName.replace(".", File.separator);
        }
        File dirs = new File(dir);
        if (!dirs.exists()) {
            return;
        }

        File[] files = dirs.listFiles();
        if (files.length == 0) {
            return;
        }
        for (File file : files) {
            try {
                String className = file.getName();
                if (className.endsWith(CLASS_REAR)) {
                    Class clazz = classLoader.loadClass(packageName + "." + className.replace(CLASS_REAR, ""));
                    doProcessor(clazz);
                }
            } catch (ClassNotFoundException e) {
                LOG.error("load class error " + file.getName(), e);
                continue;
            }
        }
    }

    protected void scanDir(String dir) {
        if (inJar(dir)) {
            scanClassInJar(dir);
        } else {
            scanClassInDir(dir);
        }
    }

    public void scanPackages(String... packageNames) {
        if (packageNames != null) {
            for (String packageName : packageNames) {
                scanPackage(packageName);
            }
        }
    }

    public void scanPackage(String packageName) {
        if (scanDirs.contains(packageName)) {
            return;
        }
        scanDirs.add(packageName);
        String packageDir = "/" + packageName.replace(".", "/");
        Set<String> hasScan = new HashSet<>();
        if (inJar(packageDir)) {
            scanClassInJar(packageDir);
        } else {
            List<String> dirs = scanPackageDir(packageDir);
            if (dirs == null) {
                return;
            }

            for (String dir : dirs) {
                if (hasScan.contains(dir)) {
                    continue;
                } else {
                    scanDir(dir);
                    hasScan.add(dir);
                }

            }
        }

    }

    protected List<String> scanPackageDir(String packageDir) {
        try {

            URL url = this.getClass().getResource(packageDir);
            if (url == null) {
                return null;
            }
            BufferedReader br = new BufferedReader(new InputStreamReader(url.openStream()));
            String line = br.readLine();
            boolean hasScane = false;
            List<String> dirs = new ArrayList<>();
            while (line != null) {
                if (!line.endsWith(CLASS_REAR)) {
                    String dir = packageDir + "/" + line;
                    File fileDir = new File(url.getPath() + "/" + line);
                    if (fileDir.isDirectory()) {
                        List<String> result = scanPackageDir(dir);
                        if (result != null) {
                            dirs.addAll(result);
                        }
                    }
                } else {
                    if (!hasScane) {
                        dirs.add(packageDir);
                        hasScane = true;
                    }
                }
                line = br.readLine();
            }
            return dirs;
        } catch (Exception e) {
            LOG.error("包扫描异常：", e);
            // throw new RuntimeException("scan error "+packageDir,e);
        }
        return null;

    }

    protected boolean inJar(String dir) {
        return FileUtil.inJar(dir, this.getClass());
    }

    protected void scanClassInJar(String dirName) {
        URL url = null;
        try {
            url = this.getClass().getResource(dirName);
            if (url == null) {
                return;
            }
        } catch (Exception e) {
            LOG.error("ScanFunctionService scanClassInJar error", e);
        }

        // jar:file:/Users/yuanxiaodong/alibaba/rule-engine-feature/5/rules-engine/engine/target/ruleengine
        // .jar!/com/aliyun/filter/function/expression

        String jarUrl = url.toString().replace("jar:file:", "");
        int index = jarUrl.indexOf("!/");
        String packageName = createPackageName(dirName);
        jarUrl = jarUrl.substring(0, index);
        scanClassInJar(jarUrl, packageName, this.getClass().getClassLoader());

    }

    protected void scanClassInJar(String jarPath, String packageName, ClassLoader classLoader) {
        try {
            if (classLoader == null) {
                classLoader = this.getClass().getClassLoader();
            }
            JarFile jarFile = new JarFile(jarPath);
            Enumeration<JarEntry> entries = jarFile.entries();

            while (entries.hasMoreElements()) {
                String className = entries.nextElement().getName().replace("/", ".");
                if (className.startsWith(packageName) && className.endsWith(".class")) {
                    className = className.replace(CLASS_REAR, "");
                    doRegisterFunction(className, classLoader);
                }

            }
        } catch (Exception e) {
            LOG.error("ScanFunctionService scanClassInJar JarFile error", e);
        }
    }

    protected void scanClassInDir(String dirName) {

        InputStream in = this.getClass().getResourceAsStream(dirName);
        if (in == null) {
            return;
        }
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String packageName = createPackageName(dirName);
        try {
            String line = br.readLine();
            while (line != null) {
                if (!line.endsWith(CLASS_REAR)) {
                    line = br.readLine();
                    continue;
                }
                String className = line.replace(CLASS_REAR, "");
                doRegisterFunction(packageName + className);
                line = br.readLine();
            }
        } catch (IOException e) {
            LOG.error("注册类错误" + e.getMessage(), e);
        }

    }

    private String createPackageName(String dirName) {
        if (dirName.startsWith("/")) {
            String packageName = dirName.substring(1).replace("/", ".") + ".";
            return packageName;
        } else {
            return this.getClass().getPackage().getName() + "." + dirName + ".";
        }
    }

    protected void doRegisterFunction(String className) {
        doRegisterFunction(className, this.getClass().getClassLoader());
    }

    protected void doRegisterFunction(String className, ClassLoader classLoader) {
        Class clazz = null;
        try {
            clazz = Class.forName(className, true, classLoader);
            doProcessor(clazz);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("初始化类错误" + e.getMessage(), e);
        }
    }

    protected abstract void doProcessor(Class clazz);
}
