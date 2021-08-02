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
package org.apache.rocketmq.streams.common.classloader;

public class ByteClassLoader extends ClassLoader {
    public ByteClassLoader(ClassLoader classLoader) {
        super(classLoader);
    }

    public Class<?> defineClass(String name, byte[] b) {
        // ClassLoader是个抽象类，而ClassLoader.defineClass 方法是protected的
        // 所以我们需要定义一个子类将这个方法暴露出来
        return super.defineClass(name, b, 0, b.length);
    }

}
