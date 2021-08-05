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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Base64;

public class Base64Utils {
    public final static Base64.Decoder decoder = Base64.getDecoder();
    final static Base64.Encoder encoder = Base64.getEncoder();

    public Base64Utils() {
    }

    public static byte[] decode(String base64) {

        try {
            return decoder.decode(base64);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public static String encode(byte[] bytes) {
        try {
            return encoder.encodeToString(bytes);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public static String encodeFile(String filePath) throws Exception {
        byte[] bytes = fileToByte(filePath);
        return encode(bytes);
    }

    public static void decodeToFile(String filePath, String base64) throws Exception {
        byte[] bytes = decode(base64);
        byteArrayToFile(bytes, filePath);
    }

    public static byte[] fileToByte(String filePath) throws Exception {
        byte[] data = new byte[0];
        File file = new File(filePath);
        if (file.exists()) {
            FileInputStream in = new FileInputStream(file);
            ByteArrayOutputStream out = new ByteArrayOutputStream(2048);
            byte[] cache = new byte[1024];

            int nRead1;
            while ((nRead1 = in.read(cache)) != -1) {
                out.write(cache, 0, nRead1);
                out.flush();
            }

            out.close();
            in.close();
            data = out.toByteArray();
        }

        return data;
    }

    public static void byteArrayToFile(byte[] bytes, String filePath) throws Exception {
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        File destFile = new File(filePath);
        if (!destFile.getParentFile().exists()) {
            destFile.getParentFile().mkdirs();
        }

        destFile.createNewFile();
        FileOutputStream out = new FileOutputStream(destFile);
        byte[] cache = new byte[1024];

        int nRead1;
        while ((nRead1 = in.read(cache)) != -1) {
            out.write(cache, 0, nRead1);
            out.flush();
        }

        out.close();
        in.close();
    }
}
