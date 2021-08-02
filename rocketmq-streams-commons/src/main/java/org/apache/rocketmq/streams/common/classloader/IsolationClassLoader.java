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
package org.apache.rocketmq.streams.common.classloader;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * 加载指定目录的class
 */
public class IsolationClassLoader extends URLClassLoader {

    public IsolationClassLoader(String libDir, ClassLoader parent) {
        super(new URL[] {}, parent);
        tryLoadJarInDir(libDir);
    }

    public IsolationClassLoader(String libDir) {
        super(new URL[] {}, null);
        tryLoadJarInDir(libDir);
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
        return super.loadClass(name);
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        try {
            return super.findClass(name);
        } catch (ClassNotFoundException e) {
            return IsolationClassLoader.class.getClassLoader().loadClass(name);
        }
    }

    private void tryLoadJarInDir(String dirPath) {
        File dir = new File(dirPath);
        // 自动加载目录下的jar包
        if (dir.exists() && dir.isDirectory()) {
            for (File file : dir.listFiles()) {
                if (file.isFile() && file.getName().endsWith(".jar") && file.getName().startsWith("operator")) {
                    this.addURL(file);
                    continue;
                }
            }
        } else if (dir.exists() && dir.getName().endsWith(".jar") && dir.getName().startsWith("operator")) {
            this.addURL(dir);
        }
    }

    private void addURL(File file) {
        try {
            super.addURL(new URL("file", null, file.getCanonicalPath()));
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
