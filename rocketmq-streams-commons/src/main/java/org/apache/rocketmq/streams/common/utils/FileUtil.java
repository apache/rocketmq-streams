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
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.JarURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
//import org.apache.http.client.config.RequestConfig;
//import org.apache.http.client.methods.CloseableHttpResponse;
//import org.apache.http.client.methods.HttpGet;
//import org.apache.http.impl.client.CloseableHttpClient;
//import org.apache.http.impl.client.HttpClients;
//import org.apache.log4j.lf5.util.StreamUtils;
import org.apache.rocketmq.streams.common.interfaces.ILineMessageProcessor;

public class FileUtil {

    private static final Log LOG = LogFactory.getLog(FileUtil.class);

    public static final String LINE_SIGN = System.getProperty("line.separator");

    public static final String LOCAL_FILE_HEADER = "file:";

    public static final String CLASS_PATH_FILE_HEADER = "classpath://";

    /**
     * 查找指定的文件
     *
     * @param targetFilePath 目录
     * @param fileName       查找的文件
     * @return
     */
    public static File findFile(String targetFilePath, String fileName) {
        return findFile(new File(targetFilePath), fileName);
    }

    public static File createFileSupportResourceFile(String fileUrl) {
        if (fileUrl.startsWith(CLASS_PATH_FILE_HEADER)) {
            fileUrl = fileUrl.replaceFirst(CLASS_PATH_FILE_HEADER, "");
            return FileUtil.getResourceFile(fileUrl);
        } else if (fileUrl.startsWith(LOCAL_FILE_HEADER)) {
            fileUrl = fileUrl.replaceFirst(LOCAL_FILE_HEADER, "");
            return new File(fileUrl);
        } else {
            return new File(fileUrl);
        }
    }

    /**
     * 获取jar包路径
     *
     * @return
     */
    public static String getJarPath() {
        String path = FileUtil.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        if (path.endsWith(".jar")) {
            int startInde = path.lastIndexOf(File.separator);
            path = path.substring(0, startInde);
            return path;
        }
        return null;
    }

    private static URL getJarFileURL(String fileUrl) {
        if (fileUrl.indexOf("jar:file") != -1) {

            try {
                return new URL(fileUrl);
            } catch (MalformedURLException e) {
                throw new RuntimeException("create url error, the string is " + fileUrl, e);
            }
        }
        if (fileUrl.startsWith(CLASS_PATH_FILE_HEADER)) {
            fileUrl = fileUrl.replaceFirst(CLASS_PATH_FILE_HEADER, "");
            URL url = PropertiesUtils.class.getClassLoader().getResource(fileUrl);
            String path = url.toString();
            if (path.indexOf("jar:file") != -1) {
                return url;
            }
        }
        return null;
    }

    public static boolean isJarFile(String fileUrl) {
        URL url = getJarFileURL(fileUrl);
        if (url != null) {
            return true;
        }
        return false;
    }

    /**
     * 查找指定的文件
     *
     * @param targetFile 目录
     * @param fileName   查找的文件
     * @return
     */
    public static File findFile(File targetFile, String fileName) {
        return findFile(targetFile, fileName, false);
    }

    public static File findFileByRegex(File targetFile, String fileName) {
        return findFile(targetFile, fileName, true);
    }

    public static boolean inJar(String dir, Class clazz) {
        URL url = null;
        try {
            url = clazz.getResource(dir);
        } catch (Exception e) {
            LOG.error("ScanFunctionService inJar error", e);
        }

        if (url == null) {
            return false;
        }
        if (url.toString().startsWith("jar:file:")) {
            return true;
        }
        return false;
    }

    public static List<File> getFileFromResoure2Target(String targetDir, String dirName, Class clazz,
        String fileNameRegex, boolean supportNesting) {
        URL url = clazz.getClassLoader().getResource(dirName);
        if (url == null) {
            return null;
        }
        return getFileFromDir2Target(targetDir, url.getFile(), fileNameRegex, supportNesting);
    }

    public static List<File> getFileFromDir2Target(String targetDir, String dirName, String fileNameRegex,
        boolean supportNesting) {
        File dir = new File(dirName);
        File[] files = dir.listFiles();
        if (files == null) {
            return null;
        }
        List<File> fileList = new ArrayList<>();
        for (File file : files) {
            if (file.isDirectory()) {
                if (!supportNesting) {
                    continue;
                }
                List<File> subFiles = getFileFromDir2Target(targetDir + File.separator + file.getName(),
                    file.getAbsolutePath(), fileNameRegex, supportNesting);
                if (subFiles != null) {
                    fileList.addAll(subFiles);
                }
                continue;
            }
            if (!StringUtil.matchRegex(file.getName(), fileNameRegex)) {
                continue;
            }
            File targetDirFile = new File(targetDir);
            if (targetDirFile.exists() == false) {
                targetDirFile.mkdirs();
            }
            copyToTarget(targetDir, file);
            fileList.add(new File(targetDir, file.getName()));
        }
        return fileList;
    }

    /**
     * 从jar包中发现指定名字的文件，支持嵌套查询
     *
     * @param dirName
     * @param clazz
     * @param fileNameRegex
     * @return
     */
    public static List<File> getFileFromJar2Target(String targetDir, String dirName, Class clazz, String fileNameRegex,
        boolean supportNesting) {
        URL url = null;
        BufferedReader br = null;
        BufferedWriter bw = null;
        try {
            url = clazz.getResource(dirName);
            if (url == null) {
                return null;
            }
            JarURLConnection jarURLConnection = (JarURLConnection) url.openConnection();
            JarFile jarFile = jarURLConnection.getJarFile();
            Enumeration<JarEntry> entries = jarFile.entries();
            List<File> fileList = new ArrayList<>();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                String fileName = entry.getName();
                if (fileName.startsWith(jarURLConnection.getEntryName()) && !fileName.endsWith("/")) {
                    if (!StringUtil.matchRegex(fileName, fileNameRegex)) {
                        continue;
                    }
                    URL fileUrl = clazz.getClassLoader().getResource(fileName);
                    if (fileUrl == null) {
                        LOG.error("can not load component's properties file " + fileUrl);
                        continue;
                    }

                    br = new BufferedReader(new InputStreamReader(fileUrl.openStream()));
                    int startIndex = fileName.indexOf('/');
                    int endIndex = fileName.lastIndexOf('/');

                    String targetPath = targetDir + fileName.substring(startIndex, endIndex);
                    String targetFileName = fileName.substring(endIndex + 1);

                    File targetDirFile = new File(targetPath);
                    if (targetDirFile.exists() == false) {
                        targetDirFile.mkdirs();
                        System.out.println("创建目标目录成功");
                    }
                    File targetFile = new File(targetPath, targetFileName);

                    System.out.println("创建目标目录成功:" + targetFile.getParent());
                    if (!targetFile.exists()) {
                        targetFile.createNewFile();
                    }
                    bw = new BufferedWriter(new FileWriter(targetFile));
                    String line = br.readLine();
                    while (line != null) {
                        bw.write(line);
                        line = br.readLine();
                    }
                    bw.flush();
                    ;
                    fileList.add(targetFile);
                }
            }
            return fileList;
        } catch (Exception e) {
            throw new RuntimeException("FileUtil getFileFromJar2Target copy file failed ", e);
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
                if (bw != null) {
                    bw.close();
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * 查找指定的文件
     *
     * @param targetFile 目录
     * @param fileName   查找的文件
     * @return
     */
    public static File findFile(File targetFile, String fileName, boolean supportRegex) {
        String targetFileName = targetFile.getName();
        if (!targetFile.isDirectory()) {
            if (isMatch(targetFileName, fileName, supportRegex)) {
                return targetFile;
            }
            return null;
        }
        File[] subFiles = targetFile.listFiles();
        for (File file : subFiles) {
            if (file.isFile()) {
                if (isMatch(file.getName(), fileName, supportRegex)) {
                    return file;
                }
            } else {
                File tmpFile = findFile(file, fileName, supportRegex);
                if (tmpFile != null) {
                    return tmpFile;
                }
            }
        }
        return null;
    }

    /**
     * 加载指定位置的属性
     *
     * @param propertiesPath
     * @return
     */
    public static File getResourceFile(String propertiesPath) {
        URL url = PropertiesUtils.class.getClassLoader().getResource(propertiesPath);
        if (url == null) {
            LOG.error("can not load component's properties file " + propertiesPath);
            return null;
        }

        return new File(url.getPath());
    }

    protected static boolean isMatch(String oriFileName, String matchedFileName, boolean supportRegex) {
        if (supportRegex) {
            return StringUtil.matchRegex(oriFileName, matchedFileName);
        } else {
            return oriFileName.equals(matchedFileName);
        }
    }

    public static File createTmpFile(String prefix) {
        String tmpDir = System.getProperty("java.io.tmpdir");
        String random = RandomStrUtil.getRandomStr(5);
        String unzipdir = FileUtil.concatFilePath(tmpDir, prefix + "_" + random);
        File dir = new File(unzipdir);
        while (dir.exists()) {
            random = RandomStrUtil.getRandomStr(5);
            unzipdir = FileUtil.concatFilePath(tmpDir, prefix + "_" + random);
            dir = new File(unzipdir);
        }
        dir.mkdirs();
        return dir;
    }

    /**
     * 把一系列文件copy到目标位置。如果目标位置有已经存在的文件，则根据是否替换标示做对应处理
     *
     * @param targetDir 目标目录
     * @return
     */
    public static boolean copyToTarget(File targetDir, File... files) {
        return copyToTarget(targetDir.getAbsolutePath(), files);
    }

    /**
     * 把一系列文件copy到目标位置。如果目标位置有已经存在的文件，则根据是否替换标示做对应处理
     *
     * @param outDir 目标目录
     * @return
     */
    public static boolean copyToTarget(String outDir, File... files) {
        if (files == null) {
            return true;
        }
        List<File> fileList = new ArrayList<>();
        for (File file : files) {
            fileList.add(file);
        }
        return copy(fileList, outDir, true);
    }

    /**
     * 把一系列文件copy到目标位置。如果目标位置有已经存在的文件，则根据是否替换标示做对应处理
     *
     * @param outDir 目标目录
     * @return
     */
    public static boolean copyToTarget(String outDir, String... fileNames) {
        if (fileNames == null) {
            return true;
        }
        List<File> fileList = new ArrayList<>();
        for (String fileName : fileNames) {
            fileList.add(new File(fileName));
        }
        return copy(fileList, outDir, true);
    }

    /**
     * 把一系列文件copy到目标位置。如果目标位置有已经存在的文件，则根据是否替换标示做对应处理
     *
     * @param fileList 文件列表
     * @param outDir   目标目录
     * @return
     */
    public static boolean copy(List<File> fileList, String outDir, boolean needReplace) {
        if (fileList == null || fileList.size() == 0) {
            throw new RuntimeException("upgrade error ,expect fileList  is not null");
        }
        if (StringUtil.isEmpty(outDir)) {
            throw new RuntimeException("upgrade error ,expect out dir is not null");
        }
        LOG.info("FileUtil copy start...fileListSize:" + fileList.size() + ",outDir:" + outDir);
        File targetDir = new File(outDir);
        if (!targetDir.exists()) {
            targetDir.mkdirs();
        }
        LOG.info("fileList size:" + fileList.size() + " ,targetDir:" + targetDir);
        for (File file : fileList) {
            boolean isFromZip = file.getAbsolutePath().contains(".zip") ? true : false;
            File dstFile = null;
            dstFile = new File(FileUtil.concatFilePath(outDir, file.getName()));
            //            if (isFromZip) {
            //                String dir = FileUtils.getOrgPreDir4upzipFile(file);
            //                dstFile = new File(FileUtil.concatFilePath(outDir, dir));
            //                throw new RuntimeException("can not support zip");
            //            } else {
            //                dstFile = new File(FileUtil.concatFilePath(outDir, file.getName()));
            //            }
            if (needReplace == false) {
                if (dstFile.exists()) {
                    LOG.debug("copy file[" + file.getAbsolutePath() + "] to dir[" + targetDir
                        + "]   dstfile also exists, so return now");
                    continue;
                }
            }
            try {
                // 如果是非压缩包，则只有一个文件。如果是压缩包则有多个文件，我们只要通过第一个文件获取zip解压后的目录将目录拷过去即可
                // 不好直接使用FileUtils.copyDirectoryToDirectory(file,targetDir);因为它不支持是否替换，另外它会拷贝整个子目录导致多次替换
                FileUtils.copyFileToDirectory(file, targetDir);
                LOG.info("copy file[" + file.getAbsolutePath() + "] to dir[" + targetDir + "]  success");
                //                if (isFromZip) {
                //                    FileUtils.copyFile(file, dstFile);
                //                    LOG.info("copy file[" + file.getAbsolutePath() + "] to file[" + dstFile
                //                    .getAbsolutePath()
                //                        + "]  success");
                //                } else {
                //                    FileUtils.copyFileToDirectory(file, targetDir);
                //                    LOG.info("copy file[" + file.getAbsolutePath() + "] to dir[" + targetDir + "]
                //                    success");
                //                }
            } catch (IOException e) {
                LOG.error("copy file[" + file.getAbsolutePath() + "] to dir[" + targetDir + "]  error "
                    + e.getMessage(), e);
                throw new RuntimeException("copy upgrade file error " + file.getAbsolutePath(), e);
            }
        }
        LOG.info("FileUtil copy all file success...");
        return true;
    }

    /**
     * 创建补丁目录
     *
     * @param baseTarget
     * @param fileName
     * @param subDirs
     * @return
     */
    public static String createPatchFilePath(String baseTarget, String fileName, String... subDirs) {
        return createPatchFilePath(baseTarget, fileName, subDirs);
    }

    public static String concatDir(String baseTarget, String... subDirs) {
        String path = baseTarget;
        if (subDirs != null) {
            for (String subDir : subDirs) {
                path = concatFilePath(path, subDir);
            }
        }
        return path;
    }

    public static String concatFilePath(String baseTarget, String fileName, String... subDirs) {
        String path = concatDir(baseTarget, subDirs);
        return concatFilePath(path, fileName);
    }

    /**
     * 拼接目录和文件名为一个完整的文件路径
     *
     * @param fileDir
     * @param fileName
     * @return
     */
    public static String concatFilePath(String fileDir, String fileName) {
        String dir = fileDir;
        if (fileDir != null) {
            dir = dir.trim();
        } else {
            dir = "";
        }
        String name = fileName;
        if (fileName != null) {
            name = fileName.trim();
        } else {
            name = "";
        }
        if (dir.endsWith(File.separator)) {
            return dir + name;
        } else {
            return dir + File.separator + name;
        }
    }

    /**
     * 加载文件内容，并返回文件的行数据，适合文件不大的场景
     *
     * @param inputStream
     * @return
     */
    public static String loadFileContent(InputStream inputStream) {
        return processFileLine(inputStream, new ILineMessageProcessor<String>() {

            StringBuilder stringBuilder = new StringBuilder();

            @Override
            public void doProcessLine(String line) {
                stringBuilder.append(line);
            }

            @Override
            public String getResult() {
                return stringBuilder.toString();
            }
        });
    }

    public static boolean write(String fileName, String... rows) {
        if (rows == null || rows.length == 0) {
            return false;
        }
        List<String> rowList = new ArrayList<>();
        for (String row : rows) {
            rowList.add(row);
        }
        return write(fileName, rowList);

    }

    public static boolean write(String fileName, List<String> rows) {
        return write(fileName, rows, false);
    }

    public static boolean write(String fileName, List<String> rows, boolean isAppend) {
        File file = createFileSupportResourceFile(fileName);
        return write(file, rows, isAppend);
    }

    public static boolean write(File file, List<String> rows, boolean isAppend) {

        BufferedWriter bw = null;
        try {
            File dir = file.getParentFile();
            if (dir.exists() == false) {
                dir.mkdirs();
            }
            if (file.exists() == false) {
                file.createNewFile();
            }
            bw = new BufferedWriter(new FileWriter(file, isAppend));
            if (rows == null || rows.size() == 0) {
                bw.flush();
                return false;
            }

            for (String row : rows) {
                bw.write(row + LINE_SIGN);
            }
            bw.flush();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("write file error " + file.getName(), e);
        } finally {
            if (bw != null) {
                try {
                    bw.close();
                } catch (IOException e) {
                    throw new RuntimeException("write file error " + file.getName(), e);
                }
            }
        }
        return true;
    }

    public static boolean append(String fileName, List<String> rows) {
        return write(fileName, rows, true);
    }

    /**
     * 加载文件内容，并返回文件的行数据，适合文件不大的场景
     *
     * @param inputStream
     * @return
     */

    public static String loadFileContentContainLineSign(InputStream inputStream) {
        return processFileLine(inputStream, new ILineMessageProcessor<String>() {

            StringBuilder stringBuilder = new StringBuilder();
            boolean isFirst = true;

            @Override
            public void doProcessLine(String line) {
                if (isFirst) {
                    isFirst = false;
                } else {
                    stringBuilder.append(LINE_SIGN);
                }
                stringBuilder.append(line);
            }

            @Override
            public String getResult() {
                return stringBuilder.toString();
            }

        });
    }

    /**
     * 加载文件内容，并返回文件的行数据，适合文件不大的场景
     *
     * @param fileName
     * @return
     */

    public static String loadFileContentContainLineSign(String fileName) {

        try {
            File file = createFileSupportResourceFile(fileName);
            FileInputStream inputStream = new FileInputStream(file);
            return loadFileContentContainLineSign(inputStream);
        } catch (FileNotFoundException e) {
            LOG.error("file not  find " + fileName, e);
            return null;
        }
    }

    /**
     * 加载文件内容，并返回文件的行数据，适合文件不大的场景
     *
     * @param fileName
     * @return
     */
    public static String loadFileContent(String fileName) {
        return loadFileContent(fileName, false);
    }

    /**
     * 加载文件内容，并返回文件的行数据，适合文件不大的场景
     *
     * @param fileName
     * @return
     */
    public static String loadFileContent(String fileName, boolean needTrim) {
        return processFileLine(fileName, new ILineMessageProcessor<String>() {

            StringBuilder stringBuilder = new StringBuilder();

            @Override
            public void doProcessLine(String line) {
                if (needTrim) {
                    line = line.trim();
                }
                stringBuilder.append(line);
            }

            @Override
            public String getResult() {
                return stringBuilder.toString();
            }

        });
    }

    /**
     * 加载文件内容，并返回文件的行数据，适合文件不大的场景
     *
     * @param inputStream
     * @return
     */
    public static List<String> loadFileLine(InputStream inputStream) {
        return processFileLine(inputStream, new ILineMessageProcessor<List<String>>() {

            List<String> result = new ArrayList<String>();

            @Override
            public void doProcessLine(String line) {
                result.add(line);
            }

            @Override
            public List<String> getResult() {
                return result;
            }
        });
    }

    /**
     * 加载文件内容，并返回文件的行数据，适合文件不大的场景
     *
     * @param fileName
     * @return
     */
    public static List<String> loadFileLine(String fileName) {
        return processFileLine(fileName, new ILineMessageProcessor<List<String>>() {

            List<String> result = new ArrayList<String>();

            @Override
            public void doProcessLine(String line) {
                result.add(line);
            }

            @Override
            public List<String> getResult() {
                return result;
            }
        });
    }

    /**
     * 加载数据，并处理每行数据，并返回结果
     *
     * @param fileName
     * @param lineProcessor
     * @param <T>
     * @return
     */
    public static <T> T processFileLine(String fileName, ILineMessageProcessor<T> lineProcessor) {
        try {
            if (isJarFile(fileName)) {
                InputStream inputStream = getJarInputStream(fileName);
                return processFileLine(inputStream, lineProcessor);
            }
            File file = createFileSupportResourceFile(fileName);
            if (file.exists() == false) {
                return null;
            }
            return processFileLine(new FileInputStream(file), lineProcessor);
        } catch (FileNotFoundException e) {
            LOG.error("file not  find " + fileName, e);
            return null;
        }
    }

    protected static InputStream getJarInputStream(String fileName) {
        if (!isJarFile(fileName)) {
            throw new RuntimeException("can not support not jar file reader " + fileName);
        }
        URL url = getJarFileURL(fileName);
        JarURLConnection jarConnection = null;
        try {
            jarConnection = (JarURLConnection) url
                .openConnection();
            InputStream in = jarConnection.getInputStream();
            return in;
        } catch (Exception e) {
            throw new RuntimeException("can not getJarInputStream not jar file reader " + fileName, e);
        }
    }

    /**
     * 加载数据，并处理每行数据，并返回结果
     *
     * @param inputStream
     * @param lineProcessor
     * @param <T>
     * @return
     */
    public static <T> T processFileLine(InputStream inputStream, ILineMessageProcessor<T> lineProcessor) {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(inputStream));
            String line = br.readLine();
            while (line != null) {
                lineProcessor.doProcessLine(line);
                line = br.readLine();
            }
            return lineProcessor.getResult();
        } catch (IOException e) {
            LOG.error("file io error ", e);
        } finally {
            try {
                br.close();
                inputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }
    //
    //    public static File getJarFile2Ramdom(String jarUrl) {
    //        String jarName = getJarFileNameByUrl(jarUrl);
    //        File tmpdir = createTmpFile("tmp");
    //
    //        File f = new File(tmpdir + File.separator + DateUtil.getCurrentTimeString("yyyyMMddHHmmss") + File
    // .separator
    //                          + RandomStringUtils.randomNumeric(4), jarName);
    //
    //        File fileParent = f.getParentFile();
    //        if (!fileParent.exists()) {
    //            fileParent.mkdirs();
    //        }
    //
    //        if (f.exists() && f.length() > 0) {
    //            return f;
    //        } else {
    //            downloadFileFromHttp(jarUrl, f);
    //            return f;
    //        }
    //    }

    //    public static File getJarFile(String jarUrl) {
    //        String jarName = getJarFileNameByUrl(jarUrl);
    //        File tmpdir = createTmpFile("tmp");
    //
    //        File f = new File(tmpdir, jarName);
    //        if (f.exists() && f.length() > 0) {
    //            return f;
    //        } else {
    //            downloadFileFromHttp(jarUrl, f);
    //            return f;
    //        }
    //    }

    public static String getJarFileNameByUrl(String jarUrl) {
        String param = jarUrl.substring(jarUrl.indexOf("?") + 1);

        String[] paramList = param.split("=");

        return paramList[1];
    }

    public static boolean deleteFile(String fileOrDir) {
        return deleteFile(new File(fileOrDir));
    }

    public static boolean deleteFile(File fileOrDir) {

        if (fileOrDir.isFile()) {
            fileOrDir.delete();
            return true;
        }
        File[] files = fileOrDir.listFiles();
        if (files == null || files.length == 0) {
            fileOrDir.delete();
            return true;
        }
        for (File f : files) {
            deleteFile(f);
        }
        fileOrDir.delete();
        return true;
    }

    //    public static File downloadFileFromHttp(String fileUrl, File file) {
    //        InputStream in = null;
    //        OutputStream out = null;
    //        try {
    //            CloseableHttpClient closeableHttpClient = new DefaultHttpClient();
    //
    //            RequestConfig requestConfig = RequestConfig.DEFAULT;
    //            setTimeOutConfig(requestConfig);
    //            HttpGet httpGet = new HttpGet(fileUrl);
    //            httpGet.setConfig(requestConfig);
    //
    //            HttpResponse httpResponse = closeableHttpClient.execute(httpGet);
    //            HttpEntity entity = httpResponse.getEntity();
    //            in = entity.getContent();
    //            out = new FileOutputStream(file);
    //
    //            Header[] headers = httpResponse.getAllHeaders();
    //            StringBuilder headerstr = new StringBuilder();
    //            for (Header header : headers) {
    //                headerstr.append(" ,headername=" + header.getName() + ",headervalue=" + header.getValue());
    //            }
    //            byte[] buffer = new byte[4096];
    //            int readLength;
    //            int count = 0;
    //            while ((readLength = in.read(buffer)) > 0) {
    //                byte[] bytes = new byte[readLength];
    //                System.arraycopy(buffer, 0, bytes, 0, readLength);
    //                out.write(bytes);
    //                count++;
    //            }
    //            out.flush();
    //            LOG.warn(" 检查： 正常结束" + headerstr + ",file.length=" + file.length() + " 检查： 复制次数：" + count);
    //        } catch (Exception e) {
    //            throw new RuntimeException(e);
    //        } finally {
    //            if (in != null) {
    //                try {
    //                    in.close();
    //                } catch (Exception e) {
    //                    throw new RuntimeException(e);
    //                }
    //            }
    //            if (out != null) {
    //                try {
    //                    out.close();
    //                } catch (Exception e) {
    //                    throw new RuntimeException(e);
    //                }
    //            }
    //        }
    //        return null;
    //    }
    //
    //    private static RequestConfig setTimeOutConfig(RequestConfig requestConfig) {
    //        return RequestConfig.copy(requestConfig).setConnectionRequestTimeout(60000).setConnectTimeout(60000)
    // .setSocketTimeout(10000).build();
    //    }

    public static List<File> createFileList(File workDir) {
        List<File> fileList = new ArrayList<>();
        if (workDir.isFile()) {
            return null;
        }
        File[] files = workDir.listFiles();
        if (files == null) {
            return null;
        }
        for (File file : files) {
            fileList.add(file);
        }
        return fileList;
    }

    /**
     * 在文件列表中找出所需要的文件
     *
     * @param fileList
     * @param fileName
     * @return
     */
    public static File findFile(List<File> fileList, String fileName) {
        if (fileList == null || fileName == null) {
            LOG.warn("FileUtil findFile error,param is null");
            return null;
        }
        for (File file : fileList) {
            if (fileName.equals(file.getName())) {
                return file;
            }
        }
        return null;
    }

    public static void main(String[] args) {
        long begin = System.currentTimeMillis();
//        File file = new File("/Users/yd/Documents/tert.jar");
//        downloadFile("https://yundun-bigdata.oss-cn-qingdao.aliyuncs.com/download/dipper/linux64/1.0.0/rocketmq-stream-sql_20220225112207911.jar",
//            file);
        downloadNet("https://yundun-bigdata.oss-cn-qingdao.aliyuncs.com/download/dipper/linux64/1.0.0/rocketmq-stream-sql_20220225112207911.jar",
            "/Users/yd/Documents/tert.jar");
        long end = System.currentTimeMillis();
        System.out.println("用时=====" + (end - begin));
    }

    /**
     * 删除文件及目录
     *
     * @param file
     * @return
     */
    public static boolean delFile(File file) {
        if (!file.exists()) {
            return false;
        }

        if (file.isDirectory()) {
            File[] files = file.listFiles();
            for (File f : files) {
                delFile(f);
            }
        }
        return file.delete();
    }

//    public static void downloadFile(String url, File destFile) {
//        if (!url.contains("http") && !url.contains("https")) {
//            url = "http://" + url;
//        }
//        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
//            HttpGet httpget = new HttpGet(url);
//            httpget.setConfig(RequestConfig.custom() //
//                .setConnectionRequestTimeout(3000) //
//                .setConnectTimeout(3000) //
//                .setSocketTimeout(3000) //
//                .build());
//            try (CloseableHttpResponse response = httpclient.execute(httpget)) {
//                org.apache.http.HttpEntity entity = response.getEntity();
////                File desc = new File(dest_file+File.separator+fileName);
//                if (!destFile.exists()) {
//                    destFile.createNewFile();
//                }
//                File folder = destFile.getParentFile();
//                folder.mkdirs();
//                try (InputStream is = entity.getContent(); //
//                     OutputStream os = new FileOutputStream(destFile)) {
//                    StreamUtils.copy(is, os);
//                }
//            }catch(Exception e){
//                throw new Throwable("文件下载失败......", e);
//            } finally {
//            }
//        } catch (Throwable e) {
//            e.printStackTrace();
//        }
////        return dest_file+File.separator+fileName;
//    }

    public static void downloadNet(String packageUrl, String destPath) {
        // 下载网络文件
        int bytesum = 0;
        int byteread = 0;

        try {
            URL url = new URL(packageUrl);
            URLConnection conn = url.openConnection();
            InputStream inStream = conn.getInputStream();
            FileOutputStream fs = new FileOutputStream(destPath);

            byte[] buffer = new byte[2048];
            int length;
            while ((byteread = inStream.read(buffer)) != -1) {
                bytesum += byteread;
                System.out.println(bytesum);
                fs.write(buffer, 0, byteread);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
