///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package org.apache.rocketmq.streams.common.stages;
//
//import org.apache.rocketmq.streams.common.classloader.ByteClassLoader;
//import org.apache.rocketmq.streams.common.topology.stages.OpenAPIChainStage;
//import org.junit.Test;
//import java.lang.reflect.ProxyGenerator;
//public class SelfChainStageTest {
//    @Test
//    public void testClassByte() {
//        OpenAPIChainStage mapUDFOperator = new OpenAPIChainStage();
//
//        byte[] classBytes = ProxyGenerator.generateProxyClass(mapUDFOperator.getClass().getName(), new Class[] {OpenAPIChainStage.class});
//        //String byteBase64= Base64Utils.encode(classBytes);
//        //byte[] bytes=Base64Utils.decode(byteBase64);
//        ByteClassLoader byteClassLoader = new ByteClassLoader(this.getClass().getClassLoader());
//        Class clazz = byteClassLoader.defineClass(null, classBytes);
//        System.out.println(clazz.getName());
//    }
//}
