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

import javax.crypto.*;
import javax.crypto.spec.DESKeySpec;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.security.Key;
import java.security.SecureRandom;

public class DESUtils {

    public DESUtils() {
    }

    public static String getSecretKey() throws Exception {
        return getSecretKey((String)null);
    }

    public static String getSecretKey(String seed) throws Exception {
        SecureRandom secureRandom;
        if (seed != null && !"".equals(seed)) {
            secureRandom = new SecureRandom(seed.getBytes());
        } else {
            secureRandom = new SecureRandom();
        }

        KeyGenerator keyGenerator = KeyGenerator.getInstance("DES");
        keyGenerator.init(secureRandom);
        SecretKey secretKey = keyGenerator.generateKey();
        return Base64Utils.encode(secretKey.getEncoded());
    }

    public static byte[] encrypt(byte[] data, String key) throws Exception {
        Key k = toKey(Base64Utils.decode(key));
        Cipher cipher = Cipher.getInstance("DES");
        cipher.init(1, k);
        return cipher.doFinal(data);
    }

    public static File encryptFile(String key, File sourceFile) throws Exception {
        File tmpFile = FileUtil.createTmpFile("");
        String tmpPath = tmpFile.getPath();
        if (!tmpPath.endsWith(File.separator)) {
            tmpPath = tmpPath + tmpPath + File.separator;
        }

        tmpPath = tmpPath + "encrypt" + File.separator;
        File encryptDir = new File(tmpPath);
        encryptDir.mkdirs();
        File destFile = new File(encryptDir, sourceFile.getName());
        if (sourceFile.exists() && sourceFile.isFile()) {
            FileInputStream in = new FileInputStream(sourceFile);
            FileOutputStream out = new FileOutputStream(destFile);
            Key k = toKey(Base64Utils.decode(key));
            Cipher cipher = Cipher.getInstance("DES");
            cipher.init(1, k);
            CipherInputStream cin = new CipherInputStream(in, cipher);
            byte[] cache = new byte[1024];

            int nRead1;
            while ((nRead1 = cin.read(cache)) != -1) {
                out.write(cache, 0, nRead1);
                out.flush();
            }

            out.close();
            cin.close();
            in.close();
        }

        return destFile;
    }

    public static byte[] decrypt(byte[] data, String key) throws Exception {
        Key k = toKey(Base64Utils.decode(key));
        Cipher cipher = Cipher.getInstance("DES");
        cipher.init(2, k);
        return cipher.doFinal(data);
    }

    public static File decryptFile(String key, File sourceFile) throws Exception {
        File tmpFile = FileUtil.createTmpFile("");
        String tmpPath = tmpFile.getPath();
        if (!tmpPath.endsWith(File.separator)) {
            tmpPath = tmpPath + tmpPath + File.separator;
        }

        tmpPath = tmpPath + "decrypt" + File.separator;
        File decryptDir = new File(tmpPath);
        decryptDir.mkdirs();
        File destFile = new File(decryptDir, sourceFile.getName());
        if (!destFile.exists()) {
            destFile.createNewFile();
        }

        if (sourceFile.exists() && sourceFile.isFile()) {
            FileInputStream in = new FileInputStream(sourceFile);
            FileOutputStream out = new FileOutputStream(destFile);
            Key k = toKey(Base64Utils.decode(key));
            Cipher cipher = Cipher.getInstance("DES");
            cipher.init(2, k);
            CipherOutputStream cout = new CipherOutputStream(out, cipher);
            byte[] cache = new byte[1024];

            int nRead1;
            while ((nRead1 = in.read(cache)) != -1) {
                cout.write(cache, 0, nRead1);
                cout.flush();
            }

            cout.close();
            out.close();
            in.close();
        }

        return destFile;
    }

    private static Key toKey(byte[] key) throws Exception {
        DESKeySpec dks = new DESKeySpec(key);
        SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("DES");
        SecretKey secretKey = keyFactory.generateSecret(dks);
        return secretKey;
    }
}
