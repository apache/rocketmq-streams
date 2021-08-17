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

import com.sun.jna.Pointer;
import com.sun.jna.platform.win32.Kernel32;
import com.sun.jna.platform.win32.WinNT;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;

/**
 * CommandUtil
 */
public class CommandUtil {

    private static Boolean isWinOS() {
        return System.getProperty("os.name").toLowerCase().startsWith("win");
    }

    private static Boolean isMacOS() {
        return System.getProperty("os.name").toLowerCase().startsWith("mac");
    }

    /**
     * 执行命令行
     *
     * @param shell
     * @return
     * @throws IOException
     */
    public static String exec(String shell) throws Exception {
        return exec(shell, null);
    }

    /**
     * 执行命令行
     *
     * @param shell
     * @param output
     * @return
     * @throws IOException
     */
    public static String exec(String shell, String output) throws Exception {
        String pid = null;
        String command = shell;
        if (StringUtil.isNotEmpty(output)) {
            command += " 1>" + output + " 2>&1";
        }
        Process process = Runtime.getRuntime().exec(new String[] {"/bin/sh", "-c", command});
        if (process != null) {
            if (process.getClass().getName().contains("UNIXProcess")) {
                // linux获取进程id
                Field pidField = process.getClass().getDeclaredField("pid");
                pidField.setAccessible(true);
                pid = String.valueOf(pidField.get(process));
            } else {
                // window获取进程id
                Field handleField = process.getClass().getDeclaredField("handle");
                handleField.setAccessible(true);
                long handle = handleField.getLong(process);
                Kernel32 kernel = Kernel32.INSTANCE;
                WinNT.HANDLE windNTHandle = new WinNT.HANDLE();
                windNTHandle.setPointer(Pointer.createConstant(handle));
                pid = String.valueOf(kernel.GetProcessId(windNTHandle));
            }
        }
        return pid;
    }

    /**
     * 根据pid判断进程是否存在，如果存在，杀掉进程，并清空文件夹
     *
     * @param pid
     * @return 0表示不存在，1表示存在，因为进程号不可能重复，所以输出肯定非0即1
     */
    public static boolean existPid(String pid) throws Exception {
        BufferedReader bufferedReader = null;
        try {
            String command = "ps -ef | awk '{print $2}' | grep -w '" + pid + "' | wc -l";
            if (isWinOS()) {
                command = "ps --no-heading " + pid + " | wc -l";
            }
            Process process = Runtime.getRuntime().exec(new String[] {"sh", "-c", command});
            if (process != null) {
                //bufferedReader用于读取Shell的输出内容
                bufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream(), "gbk"), 1024);
                process.waitFor();
            }
            String line;
            //读取Shell的输出内容，并添加到stringBuffer中
            StringBuilder output = new StringBuilder();
            while (bufferedReader != null && (line = bufferedReader.readLine()) != null) {
                output.append(line).append("\n");
            }
            int outputNum = Integer.valueOf(output.toString().trim());
            return outputNum == 1;
        } finally {
            if (bufferedReader != null) {
                try {
                    bufferedReader.close();
                } catch (Exception e) {
                    // ignore exception
                }
            }
        }
    }

    /**
     * 杀掉进程
     *
     * @param pid
     * @return true, 命令执行成功
     */
    public static void killByPid(String pid) throws Exception {
        if (existPid(pid)) {
            String command = "kill -9 " + pid;
            Process process = Runtime.getRuntime().exec(new String[] {"sh", "-c", command});
            process.waitFor();
            process.destroy();
        }
    }

    public static String calcMd5(String filePath) throws Exception {
        String cmd = String.format("md5sum %s | awk '{print $1}'", filePath);
        if (isMacOS()) {
            cmd = String.format("md5sum %s | awk '{print $4}'", filePath);
        }
        String output = filePath + ".md5";
        String pid = exec(cmd, output);

        int count = 0;
        while (existPid(pid)) {
            count++;
            if (count > 3600) {
                // 超过1小时
                throw new Exception("calculate file md5 timeout, file:" + filePath);
            }
            Thread.sleep(5000);
        }
        String md5 = IOUtil.readFileToString(new File(output), "UTF-8").trim();
        IOUtil.forceDelete(new File(output));
        return md5;
    }

    public static String calcSha256(String filePath) throws Exception {
        String cmd = String.format("sha256sum %s | awk '{print $1}'", filePath);
        if (isMacOS()) {
            cmd = String.format("sha256sum %s | awk '{print $4}'", filePath);
        }
        String output = filePath + ".sha256";
        String pid = exec(cmd, output);

        int count = 0;
        while (existPid(pid)) {
            count++;
            if (count > 3600) {
                // 超过1小时
                throw new Exception("calculate file sha256 timeout, file:" + filePath);
            }
            Thread.sleep(5000);
        }
        String md5 = IOUtil.readFileToString(new File(output), "UTF-8").trim();
        IOUtil.forceDelete(new File(output));
        return md5;
    }
}
