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
package org.apache.rocketmq.streams.common.compiler;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import org.apache.rocketmq.streams.common.utils.FileUtil;

/**
 * 编译java源码
 */
public class CustomJavaCompiler {
    //类全名
    private String fullClassName;
    private String sourceCode;
    //存放编译之后的字节码(key:类全名,value:编译之后输出的字节码)
    private Map<String, ByteJavaFileObject> javaFileObjectMap = new ConcurrentHashMap<>();
    //获取java的编译器
    private JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    //存放编译过程中输出的信息
    private DiagnosticCollector<JavaFileObject> diagnosticsCollector = new DiagnosticCollector<>();
    //执行结果（控制台输出的内容）
    private String runResult;
    //编译耗时(单位ms)
    private long compilerTakeTime;
    //运行耗时(单位ms)
    private long runTakeTime;

    private static Pattern pattern1 = Pattern.compile("package\\s+\\S+\\s*;");

    private static Pattern pattern2 = Pattern.compile("class\\s+\\S+.*\\{");

    /**
     * @param sourceFile java 文件
     */
    public CustomJavaCompiler(File sourceFile) {
        this(FileUtil.loadFileContent(sourceFile.getAbsolutePath()));
    }

    /**
     * @param sourceCode java 源码的字符串形式
     */
    public CustomJavaCompiler(String sourceCode) {
        this.sourceCode = sourceCode;
        this.fullClassName = getFullClassName(sourceCode);
    }

    public <T> T compileAndNewInstance() {
        try {
            Class aClass = compileClass();
            return (T)aClass.newInstance();
        } catch (Exception e) {
            throw new RuntimeException("compile class error, the class name is " + fullClassName, e);
        }
    }

    public Class compileClass() {
        compiler();
        StringClassLoader scl = new StringClassLoader();
        Class<?> aClass = null;
        try {
            aClass = scl.findClass(fullClassName);
            if (aClass == null) {
                return null;
            }
            return aClass;
        } catch (Exception e) {
            throw new RuntimeException("compile class error, the class name is " + fullClassName, e);
        }
    }

    /**
     * 编译字符串源代码,编译失败在 diagnosticsCollector 中获取提示信息
     *
     * @return true:编译成功 false:编译失败
     */
    protected boolean compiler() {
        long startTime = System.currentTimeMillis();
        //标准的内容管理器,更换成自己的实现，覆盖部分方法
        StandardJavaFileManager standardFileManager = compiler.getStandardFileManager(diagnosticsCollector, null, null);
        JavaFileManager javaFileManager = new StringJavaFileManage(standardFileManager);
        //构造源代码对象
        JavaFileObject javaFileObject = new StringJavaFileObject(fullClassName, sourceCode);
        //获取一个编译任务
        JavaCompiler.CompilationTask task = compiler.getTask(null, javaFileManager, diagnosticsCollector, null, null, Arrays
            .asList(javaFileObject));
        //设置编译耗时
        compilerTakeTime = System.currentTimeMillis() - startTime;
        return task.call();
    }

    /**
     * @return 编译信息(错误 警告)
     */
    public String getCompilerMessage() {
        StringBuilder sb = new StringBuilder();
        List<Diagnostic<? extends JavaFileObject>> diagnostics = diagnosticsCollector.getDiagnostics();
        for (Diagnostic diagnostic : diagnostics) {
            sb.append(diagnostic.toString()).append("\r\n");
        }
        return sb.toString();
    }

    /**
     * @return 控制台打印的信息
     */
    public String getRunResult() {
        return runResult;
    }

    public long getCompilerTakeTime() {
        return compilerTakeTime;
    }

    public long getRunTakeTime() {
        return runTakeTime;
    }

    /**
     * 获取类的全名称
     *
     * @param sourceCode 源码
     * @return 类的全名称
     */
    public static String getFullClassName(String sourceCode) {
        String className = "";
        Matcher matcher = pattern1.matcher(sourceCode);
        if (matcher.find()) {
            className = matcher.group().replaceFirst("package", "").replace(";", "").trim() + ".";
        }
        matcher = pattern2.matcher(sourceCode);
        if (matcher.find()) {
            className += matcher.group().replaceFirst("class", "").replace("{", "").trim();
        }
        int index = className.indexOf(" ");
        if (index != -1) {
            //如果类有实现接口或继承等，只取类名部分
            className = className.substring(0, index);
        }
        return className;
    }

    /**
     * 自定义一个字符串的源码对象
     */
    private class StringJavaFileObject extends SimpleJavaFileObject {
        //等待编译的源码字段
        private String contents;

        //java源代码 => StringJavaFileObject对象 的时候使用
        public StringJavaFileObject(String className, String contents) {
            super(URI.create("string:///" + className.replaceAll("\\.", "/") + Kind.SOURCE.extension), Kind.SOURCE);
            this.contents = contents;
        }

        //字符串源码会调用该方法
        @Override
        public CharSequence getCharContent(boolean ignoreEncodingErrors) throws IOException {
            return contents;
        }

    }

    /**
     * 自定义一个编译之后的字节码对象
     */
    private class ByteJavaFileObject extends SimpleJavaFileObject {
        //存放编译后的字节码
        private ByteArrayOutputStream outPutStream;

        public ByteJavaFileObject(String className, Kind kind) {
            super(URI.create("string:///" + className.replaceAll("\\.", "/") + Kind.SOURCE.extension), kind);
        }

        //StringJavaFileManage 编译之后的字节码输出会调用该方法（把字节码输出到outputStream）
        @Override
        public OutputStream openOutputStream() {
            outPutStream = new ByteArrayOutputStream();
            return outPutStream;
        }

        //在类加载器加载的时候需要用到
        public byte[] getCompiledBytes() {
            return outPutStream.toByteArray();
        }
    }

    /**
     * 自定义一个JavaFileManage来控制编译之后字节码的输出位置
     */
    private class StringJavaFileManage extends ForwardingJavaFileManager {
        StringJavaFileManage(JavaFileManager fileManager) {
            super(fileManager);
        }

        //获取输出的文件对象，它表示给定位置处指定类型的指定类。
        @Override
        public JavaFileObject getJavaFileForOutput(Location location, String className, JavaFileObject.Kind kind, FileObject sibling) throws IOException {
            ByteJavaFileObject javaFileObject = new ByteJavaFileObject(className, kind);
            javaFileObjectMap.put(className, javaFileObject);
            return javaFileObject;
        }
    }

    /**
     * 自定义类加载器, 用来加载动态的字节码
     */
    private class StringClassLoader extends ClassLoader {
        @Override
        protected Class<?> findClass(String name) throws ClassNotFoundException {
            ByteJavaFileObject fileObject = javaFileObjectMap.get(name);
            if (fileObject != null) {
                byte[] bytes = fileObject.getCompiledBytes();
                return defineClass(name, bytes, 0, bytes.length);
            }
            try {
                return ClassLoader.getSystemClassLoader().loadClass(name);
            } catch (Exception e) {
                return super.findClass(name);
            }
        }
    }

}