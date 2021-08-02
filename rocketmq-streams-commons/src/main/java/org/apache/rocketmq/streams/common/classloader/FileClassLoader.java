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

import java.io.*;

/**
 * 加载指定目录的class
 */
public class FileClassLoader extends ClassLoader {

    protected String dir;

    public FileClassLoader(String dir, ClassLoader parent) {
        super(parent);
        this.dir = dir;
    }

    //用于寻找类文件
    @Override
    public Class findClass(String name) {
        byte[] b = loadClassData(name);
        return defineClass(name, b, 0, b.length);
    }

    //用于加载类文件
    private byte[] loadClassData(String fileName) {
        String classDir = dir;
        if (!dir.endsWith(File.separator)) {
            classDir += File.separator;
        }
        if (fileName.indexOf(".") != -1) {
            fileName = fileName.replace(".", File.separator);
        }
        String name = classDir + fileName + ".class";

        //使用输入流读取类文件
        InputStream in = null;
        //使用byteArrayOutputStream保存类文件。然后转化为byte数组
        ByteArrayOutputStream out = null;
        try {
            in = new FileInputStream(new File(name));
            out = new ByteArrayOutputStream();
            int i = 0;
            while ((i = in.read()) != -1) {
                out.write(i);
            }

        } catch (Exception e) {
        } finally {
            try {
                out.close();
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        return out.toByteArray();

    }

    public static void main(String[] args) throws ClassNotFoundException {
        FileClassLoader fileClassLoader = new FileClassLoader("/tmp", FileClassLoader.class.getClassLoader());
        Class clazz = fileClassLoader.loadClass("com.aliyun.yundun.dipper.channel.self.UpgradeFunction");
        System.out.println(clazz.getName());
    }
}
