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
package org.apache.rocketmq.streams.common.utils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;

public class JarUtil {

    public static void replaceJarFile(String jarPath, Map<String, String> jarFilePath2FilePath) {
        Map<String, String> jarFilePath2Content = new HashMap<>();
        Iterator<Map.Entry<String, String>> it = jarFilePath2FilePath.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, String> entry = it.next();
            String jarFilePath = entry.getKey();
            String filePath = entry.getValue();
            String content = FileUtil.loadFileContentContainLineSign(filePath);
            jarFilePath2Content.put(jarFilePath, content);
        }
        modifyJarFile(jarPath, jarFilePath2Content);
    }

    /**
     * 替换掉jar 文件，
     *
     * @param jarPath     jar包的路径
     * @param jarFilePath jar文件在jar包的路径
     * @param filePath    要替换的jar文件
     */
    public static void replaceJarFile(String jarPath, String jarFilePath, String filePath) {
        String content = FileUtil.loadFileContentContainLineSign(filePath);
        modifyJarFile(jarPath, jarFilePath, content);

    }

    public static void modifyJarFile(String jarPath, String jarFilePath, String content) {
        Map<String, String> jarFilePath2Content = new HashMap<>();
        jarFilePath2Content.put(jarFilePath, content);
        modifyJarFile(jarPath, jarFilePath2Content);
    }

    public static void modifyJarFile(String jarPath, Map<String, String> jarFilePath2Content) {
        try {
            File file = new File(jarPath);
            JarFile jarFile = new JarFile(file);// 通过jar包的路径 创建Jar包实例
            change(jarFile, jarFilePath2Content);

        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("modifyJarFile error, the jarPath is " + jarPath, e);
        }

    }

    public static void change(JarFile jarFile, String jarFilePath, String content) {
        Map<String, String> jarFilePath2Content = new HashMap<>();
        jarFilePath2Content.put(jarFilePath, content);
        change(jarFile, jarFilePath2Content);
    }

    public static void change(JarFile jarFile, Map<String, String> jarFilePath2Content) {
        try {
            //获取entries集合lists
            List<JarEntry> lists = new LinkedList<>();
            Enumeration<JarEntry> entries = jarFile.entries();
            while (entries.hasMoreElements()) {
                JarEntry jarEntry = entries.nextElement();
                lists.add(jarEntry);
            }
            writeFile(lists, jarFile, jarFilePath2Content);// 将修改后的内容写入jar包中的指定文件

        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("modifyJarFile error, the jarPath is " + jarFile.getName(), e);
        } finally {
            try {
                jarFile.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    private static void writeFile(List<JarEntry> lists,
        JarFile jarFile, Map<String, String> jarFilePath2Content) throws IOException {
        FileOutputStream fos = new FileOutputStream(jarFile.getName(), true);
        JarOutputStream jos = new JarOutputStream(fos);
        try {
            for (JarEntry je : lists) {
                if (jarFilePath2Content.containsKey(je.getName())) {
                    String jarFilePath = je.getName();
                    String content = jarFilePath2Content.get(jarFilePath);
                    jos.putNextEntry(new JarEntry(jarFilePath));
                    jos.write(content.getBytes());
                } else {
                    //表示将该JarEntry写入jar文件中 也就是创建该文件夹和文件
                    jos.putNextEntry(new JarEntry(je));
                    jos.write(streamToByte(jarFile.getInputStream(je)));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 关闭流
            jos.close();
        }
    }

    private static byte[] streamToByte(InputStream inputStream) {
        ByteArrayOutputStream outSteam = new ByteArrayOutputStream();
        try {
            byte[] buffer = new byte[1024];
            int len;
            while ((len = inputStream.read(buffer)) != -1) {
                outSteam.write(buffer, 0, len);
            }
            outSteam.close();
            inputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return outSteam.toByteArray();
    }

    public static void main(String[] args) throws IOException {

    }

}
