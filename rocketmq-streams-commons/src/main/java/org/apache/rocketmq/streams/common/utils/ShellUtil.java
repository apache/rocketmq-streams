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
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.component.ComponentCreator;

public class ShellUtil {

    private static final String SEPERATOR_LINE = "\r\n";
    private static final Log LOG = LogFactory.getLog(ShellUtil.class);
    private static final String ENV_CMD = "env";

    public static Map<String, String> exeENV() {
        Map<String, String> result = new HashMap<>();
        String envs = runShell(ENV_CMD, true);
        String[] values = envs.split(SEPERATOR_LINE);
        if (values == null) {
            return result;
        }
        for (String line : values) {
            int startIndex = line.indexOf("=");
            if (startIndex == -1) {
                continue;
            }
            String key = line.substring(0, startIndex);
            String value = line.substring(startIndex + 1);
            if (StringUtil.isEmpty(key) || StringUtil.isEmpty(value)) {
                continue;
            }
            result.put(key, value);
        }

        return result;
    }

    public static String runShell(String... shStrs) {
        String cmd = null;
        if (shStrs == null) {
            return null;
        }
        for (String shell : shStrs) {
            if (cmd == null) {
                cmd = shell;
            } else {
                cmd += " && " + shell;
            }
        }
        return runShell(cmd);
    }

    public static String runShell(String shStr) {
        return runShell(shStr, false);
    }

    public static String runShell(String shStr, boolean needReturnValue, String runFile) {
        BufferedReader read = null;
        LOG.info("run shell command is " + shStr);
        try {
            Process process;
            String osType = System.getProperty("os.name");
            if ("Windows 10".equalsIgnoreCase(osType)) {
                process = Runtime.getRuntime().exec(shStr.split(" "), null, new File(runFile));
            } else {
                process = Runtime.getRuntime().exec(new String[] {"/bin/sh", "-c", shStr}, null, new File(runFile));
            }

            read = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line = null;
            String result = "";
            while ((line = read.readLine()) != null && needReturnValue) {
                if (needReturnValue) {
                    result += line + SEPERATOR_LINE;
                }
                LOG.info(line);
            }
            process.waitFor();
            return result;
        } catch (Exception e) {
            throw new RuntimeException("exe shell error " + shStr, e);
        } finally {
            if (read != null) {
                try {
                    read.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                ;
            }
        }
    }

    public static String runShell(String shStr, boolean needReturnValue) {
        BufferedReader read = null;
        LOG.info("run shell command is " + shStr);
        try {
            Process process;
            String osType = System.getProperty("os.name");
            if ("Windows 10".equalsIgnoreCase(osType)) {
                process = Runtime.getRuntime().exec(shStr.split(" "), null,
                    new File(ComponentCreator.getProperties().getProperty("siem.python.workdir")));
            } else {
                process = Runtime.getRuntime().exec(new String[] {"/bin/sh", "-c", shStr});
            }

            read = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line = null;
            String result = "";
            while ((line = read.readLine()) != null && needReturnValue) {
                if (needReturnValue) {
                    result += line + SEPERATOR_LINE;
                }
                LOG.info(line);
            }
            process.waitFor();
            return result;
        } catch (Exception e) {
            throw new RuntimeException("exe shell error " + shStr, e);
        } finally {
            if (read != null) {
                try {
                    read.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                ;
            }
        }
    }

    public static String runShellAndGetV(String shStr, String key) {
        try {
            Process process;
            process = Runtime.getRuntime().exec(new String[] {"/bin/sh", "-c", shStr});
            BufferedReader read = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line = null;
            String result = "";
            String result_v = "";
            while ((line = read.readLine()) != null) {
                String tmp = key + "=";
                if (line.startsWith(tmp)) {
                    result_v = line.substring(line.indexOf(tmp) + tmp.length());
                }
                result += line + "\r\n";
            }
            process.waitFor();
            LOG.debug("[total env]\n" + result + "\n[find env]\n" + result_v);
            return result_v;
        } catch (Exception e) {
            throw new RuntimeException("exe shell error " + shStr, e);
        }
    }

    public static String runShellFile(String shellFilePath) {
        String cmd = "sh " + shellFilePath;
        return runShell(cmd);
    }

    public static void main(String[] args) throws InterruptedException {
        System.out.println(ENVUtile.getENVParameter("http_proxy"));
        String result = runShell("/bin/sh export https_proxy=http://100.127.2.62:8888;export http_proxy=http://100.127.2.62:8888;export no_proxy='100.0.20.21,*.hljsjt-could.com,127.0.0.1,localhost,kinsmanbase.mysql.minirds.ops"
            + ".hljsjt-could.com,baxia.mysql.minirds.ops.hljsjt-could.com,secureservice.res.hljsjt-could.com'", true);
        System.out.println(ENVUtile.getENVParameter("http_proxy"));
        System.out.println(result);

    }
}
