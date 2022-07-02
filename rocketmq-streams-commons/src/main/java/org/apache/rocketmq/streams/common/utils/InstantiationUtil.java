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
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.util.HashMap;
import org.nustaq.serialization.FSTConfiguration;

/**
 * 因为匿名类，不能很好的序列化，目前用java的序列化框架，单写一个序列化方法实现
 *
 * @return
 */
public class InstantiationUtil {
    private static FSTConfiguration conf = FSTConfiguration.createAndroidDefaultConfiguration();

    /**
     * 因为匿名类，不能很好的序列化，目前用java的序列化框架，单写一个序列化方法实现
     *
     * @return
     */
    public static byte[] serializeObject(Object o) {
       return conf.asByteArray(o);
//        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
//             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
//            oos.writeObject(o);
//            oos.flush();
//            return baos.toByteArray();
//        } catch (IOException e) {
//            throw new RuntimeException("serializeAnonymousObject error  ", e);
//        }
    }

    /**
     * 反序列化
     *
     * @param bytes
     * @param <T>
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public static <T> T deserializeObject(byte[] bytes) {
        return (T)conf.asObject(bytes);
//
//        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
//        try {
//            ByteArrayInputStream in = new ByteArrayInputStream(bytes);
//            ObjectInputStream oois = new ClassLoaderObjectInputStream(in, classLoader);
//            Thread.currentThread().setContextClassLoader(classLoader);
//            return (T)oois.readObject();
//        } catch (IOException e) {
//            throw new RuntimeException("serializeAnonymousObject error  ", e);
//        } catch (ClassNotFoundException e) {
//            throw new RuntimeException("serializeAnonymousObject error  ", e);
//        } finally {
//            Thread.currentThread().setContextClassLoader(classLoader);
//        }
    }

    public static class ClassLoaderObjectInputStream extends ObjectInputStream {
        protected final ClassLoader classLoader;
        private static final HashMap<String, Class<?>> primitiveClasses = new HashMap(9);

        public ClassLoaderObjectInputStream(InputStream in, ClassLoader classLoader) throws IOException {
            super(in);
            this.classLoader = classLoader;
        }

        @Override
        protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
            if (this.classLoader != null) {
                String name = desc.getName();

                try {
                    return Class.forName(name, false, this.classLoader);
                } catch (ClassNotFoundException var5) {
                    Class<?> cl = (Class)primitiveClasses.get(name);
                    if (cl != null) {
                        return cl;
                    } else {
                        throw var5;
                    }
                }
            } else {
                return super.resolveClass(desc);
            }
        }

        static {
            primitiveClasses.put("boolean", Boolean.TYPE);
            primitiveClasses.put("byte", Byte.TYPE);
            primitiveClasses.put("char", Character.TYPE);
            primitiveClasses.put("short", Short.TYPE);
            primitiveClasses.put("int", Integer.TYPE);
            primitiveClasses.put("long", Long.TYPE);
            primitiveClasses.put("float", Float.TYPE);
            primitiveClasses.put("double", Double.TYPE);
            primitiveClasses.put("void", Void.TYPE);
        }
    }
}
