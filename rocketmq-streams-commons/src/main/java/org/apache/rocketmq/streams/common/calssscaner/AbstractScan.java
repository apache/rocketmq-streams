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
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import org.apache.rocketmq.streams.common.classloader.IsolationClassLoader;
import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.loader.archive.Archive;
import org.springframework.boot.loader.archive.JarFileArchive;
import org.springframework.boot.loader.jar.Handler;

/**
 * 可以扫描指定位置的类，支持目录和package扫描
 */
public abstract class AbstractScan {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractScan.class);

    private static final String CLASS_REAR = ".class";

    protected Set<String> scanDirs = new HashSet<>();

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        AbstractScan scan = new AbstractScan() {
            @Override
            protected void doProcessor(Class clazz, String functionName) {
                System.out.println(clazz.getName());
            }
        };
        scan.scanClassInJar(new URL("jar:file:/Users/fgm0129/Desktop/stream-engine.jar!/BOOT-INF/lib/rsqldb-parser-1.0.3_siem.industry-SNAPSHOT.jar!/com/alibaba/rsqldb/parser/parser/sqlnode"), "com/alibaba/rsqldb/parser/parser/sqlnode".replace("/", "."), AbstractScan.class.getClassLoader(), null);
//        scan.scanClassInJar(new URL("jar:file:/Users/yuanxiaodong/Downloads/tmp/et-industry-stream-engine.jar!/BOOT-INF/lib/rsqldb-parser-1.0.3_siem.industry-SNAPSHOT.jar!/com/alibaba/rsqldb/parser/parser/sqlnode"),"com/alibaba/rsqldb/parser/parser/sqlnode".replace("/","."),AbstractScan.class.getClassLoader(),"xxx");
    }

    public void scanJarsFromDir(String dir, String packageName) {
        IsolationClassLoader classLoader = new IsolationClassLoader(dir);
        File file = new File(dir);
        if (!file.exists()) {
            return;
        }
        if (!file.isDirectory()) {
            return;
        }
        File[] jars = file.listFiles();
        if (jars != null) {
            for (File jar : jars) {
                if (!jar.getName().endsWith(".jar")) {
                    continue;
                }
                scanClassDir(jar, packageName, classLoader, null);
            }
        }
    }

    public void scanClassDir(File jarFile, String packageName, ClassLoader classLoader, String functionName) {
        try {
            scanClassInJar(new URL("jar:file:" + jarFile.getAbsolutePath()), packageName, classLoader, functionName);
        } catch (Exception e) {
            throw new RuntimeException("url parse error " + jarFile.getAbsolutePath(), e);
        }

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
        if (files != null) {
            if (files.length == 0) {
                return;
            }
            for (File file : files) {
                try {
                    String className = file.getName();
                    if (className.endsWith(CLASS_REAR)) {
                        Class<?> clazz = classLoader.loadClass(packageName + "." + className.replace(CLASS_REAR, ""));
                        doProcessor(clazz, null);
                    }
                } catch (ClassNotFoundException e) {
                    LOG.error("load class error " + file.getName(), e);
                }
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
        String packageName = createPackageName(dirName);
        scanClassInJar(url, packageName, this.getClass().getClassLoader(), null);
    }

    protected void scanClassInJar(String packageName, ClassLoader classLoader, String functionName) {
        try {
            if (classLoader == null) {
                classLoader = this.getClass().getClassLoader();
            }
            doRegisterFunction(functionName, packageName, classLoader);
        } catch (Exception e) {
            LOG.error("ScanFunctionService scanClassInJar JarFile error", e);
        }
    }

    protected void scanClassInJar(URL url, String packageName, ClassLoader classLoader, String functionName) {
        try {
            if (classLoader == null) {
                classLoader = this.getClass().getClassLoader();
            }
            if (isNestJar(url)) {
                Handler handler = new Handler();
                org.springframework.boot.loader.jar.JarFile root = handler.getRootJarFileFromUrl(url);
                int startIndex = url.toString().indexOf("!/");
                int endIndex = url.toString().lastIndexOf("!/");
                String jarPath = url.toString().substring(startIndex + 2, endIndex);
                JarFileArchive jarFileArchive = new JarFileArchive(root);
                //过滤Jar包
                Iterator<Archive> jarFileIterator = jarFileArchive.getNestedArchives(entry -> {
                    if (entry.getName().equals(jarPath)) {
                        System.out.println(jarPath);
                        return true;
                    }
                    return false;
                }, null);
                final ClassLoader jarClassLoader = classLoader;
                while (jarFileIterator.hasNext()) {
                    JarFileArchive archive = (JarFileArchive) jarFileIterator.next();
                    //过滤class
                    archive.iterator().forEachRemaining(entry -> {
                        String className = entry.getName().replace("/", ".");
                        if (className.startsWith(packageName) && className.endsWith(".class")) {
                            className = className.replace(CLASS_REAR, "");
                            doRegisterFunction(functionName, className, jarClassLoader);
                        }
                    });
                }
            } else {
                String jarPath = url.toString().replace("jar:file:", "");
                int index = jarPath.indexOf("!/");
                jarPath = jarPath.substring(0, index);
                JarFile jarFile = new JarFile(jarPath);
                Enumeration<JarEntry> entries = jarFile.entries();
                while (entries.hasMoreElements()) {
                    String className = entries.nextElement().getName().replace("/", ".");
                    if (className.startsWith(packageName) && className.endsWith(".class")) {
                        className = className.replace(CLASS_REAR, "");
                        doRegisterFunction(functionName, className, classLoader);
                    }

                }
            }

        } catch (Exception e) {
            LOG.error("ScanFunctionService scanClassInJar JarFile url:{},packageName:{},functionName:{},error:{}", url.getPath(), packageName, functionName, e);
            throw new RuntimeException("ScanFunctionService scanClassInJar JarFile error", e);
        }
    }

    protected boolean isNestJar(URL url) {
        String urlStr = url.toString();
        int startIndex = urlStr.indexOf("!/");
        int endIndex = urlStr.lastIndexOf("!/");
        if (startIndex == endIndex) {
            return false;
        }
        return true;
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

    protected String createPackageName(String dirName) {
        if (dirName.startsWith("/")) {
            return dirName.substring(1).replace("/", ".") + ".";
        } else {
            return this.getClass().getPackage().getName() + "." + dirName + ".";
        }
    }

    protected void doRegisterFunction(String className) {
        doRegisterFunction(null, className, this.getClass().getClassLoader());
    }

    protected void doRegisterFunction(String functionName, String className, ClassLoader classLoader) {
        Class<?> clazz = null;
        try {
            clazz = Class.forName(className, true, classLoader);
            doProcessor(clazz, functionName);
        } catch (Exception e) {
            LOG.error("初始化类错误" + e.getMessage(), e);
        }
    }

    protected abstract void doProcessor(Class clazz, String functionName);

}
