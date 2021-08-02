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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * CompressUtil
 */
public class CompressUtil {

    private static final int BUFFER = 4096;

    public static void zip(String zipFileName, String folderName, File[] files) throws IOException {
        zip(zipFileName, folderName, CollectionUtil.asList(files));
    }

    public static void zip(String zipFileName, String folderName, List<File> files) throws IOException {
        ZipOutputStream out = new ZipOutputStream(new FileOutputStream(zipFileName));

        try {
            for (File file : files) {
                out.putNextEntry(new ZipEntry(folderName + "/" + file.getName()));
                int count;
                byte data[] = new byte[BUFFER];
                FileInputStream in = new FileInputStream(file);
                while ((count = in.read(data, 0, BUFFER)) != -1) {
                    out.write(data, 0, count);
                }
                in.close();
            }
        } finally {
            out.close();
        }
    }

    /**
     public static void gzip(String src, String dest) throws IOException {
     FileInputStream is = null;
     FileOutputStream os = null;
     GZIPOutputStream gos = null;
     try {
     is = new FileInputStream(src);
     os = new FileOutputStream(dest);
     gos = new GZIPOutputStream(os);

     int count;
     byte data[] = new byte[BUFFER];
     while ((count = is.read(data, 0, BUFFER)) != -1) {
     gos.write(data, 0, count);
     }

     gos.finish();
     gos.flush();
     } finally {
     if (gos != null) {
     gos.close();
     }
     if (os != null) {
     os.close();
     }
     if (is != null) {
     is.close();
     }
     }
     }
     **/
}
