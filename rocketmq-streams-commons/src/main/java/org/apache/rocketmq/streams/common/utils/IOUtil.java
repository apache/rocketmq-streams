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

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import org.apache.commons.io.FileUtils;

public class IOUtil extends FileUtils {

    public static String in2Str(InputStream in, String encoding) {
        if (in == null) {
            return null;
        }

        StringBuilder sb = new StringBuilder();
        try {
            InputStreamReader reader = new InputStreamReader(in, encoding);
            int tmp = -1;
            char temp;
            while ((tmp = reader.read()) != -1) {
                temp = (char)tmp;
                sb.append(temp);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return sb.toString();
    }

    /**
     * 将输入流(InputStream)转为byte数组，读取完后输入流(InputStream)即被关闭
     *
     * @param in 输入流
     * @return 返回byte数组
     * @throws IOException
     */
    public static byte[] in2Bytes(InputStream in) throws IOException {
        if (in == null) {
            return null;
        }

        ByteArrayOutputStream out = null;
        byte[] bytes = null;
        try {
            out = new ByteArrayOutputStream();
            byte[] buffer = new byte[2048];
            int len;
            while ((len = in.read(buffer)) > 0) {
                out.write(buffer, 0, len);
            }
            bytes = out.toByteArray();
        } finally {
            if (out != null) {
                out.close();
            }
            in.close();
        }

        return bytes;
    }

    public static long getLineCount(File file) throws IOException {
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            int line = 0;
            while (reader.readLine() != null) {
                line++;
            }
            return line;
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
    }
}
