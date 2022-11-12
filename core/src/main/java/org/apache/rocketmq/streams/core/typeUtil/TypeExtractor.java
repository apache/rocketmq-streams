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
package org.apache.rocketmq.streams.core.typeUtil;

import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.List;

public class TypeExtractor {

    public static TypeWrapper find(Object function, String name) {
        Class<?> functionClass = function.getClass();

        Method[] declaredMethods = functionClass.getDeclaredMethods();
        for (Method declaredMethod : declaredMethods) {
            if (!declaredMethod.getName().equals(name)) {
                continue;
            }

            Class<?>[] parameterTypes = declaredMethod.getParameterTypes();
            Class<?> returnType = declaredMethod.getReturnType();

            int parameterTypesIsObjectTypeNum = 0;
            for (Class<?> parameterType : parameterTypes) {
                if (parameterType == Object.class) {
                    parameterTypesIsObjectTypeNum++;
                }
            }

            if (returnType == Object.class && parameterTypesIsObjectTypeNum == parameterTypes.length) {
                continue;
            }
            return new TypeWrapper(parameterTypes, returnType);
        }

        return null;
    }


    public static TypeWrapper firstParameterSmart(Object function, String methodName) {
        String functionClassName = function.getClass().getName();
        int lambdaMarkerIndex = functionClassName.indexOf("$$Lambda$");
        if (lambdaMarkerIndex == -1) { // Not a lambda
            return find(function, methodName);
        }

        String declaringClassName = functionClassName.substring(0, lambdaMarkerIndex);
        int lambdaIndex = Integer.parseInt(functionClassName.substring(lambdaMarkerIndex + 9, functionClassName.lastIndexOf('/')));

        Class<?> declaringClass;
        try {
            declaringClass = Class.forName(declaringClassName);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Unable to find lambda's parent class " + declaringClassName);
        }

        for (Method method : declaringClass.getDeclaredMethods()) {
            if (method.isSynthetic()
                    && method.getName().startsWith("lambda$")
                    && method.getName().endsWith("$" + lambdaIndex)
                    && Modifier.isStatic(method.getModifiers())) {
                Class<?>[] parameterTypes = method.getParameterTypes();
                Class<?> returnType = method.getReturnType();
                return new TypeWrapper(parameterTypes, returnType);
            }
        }

        throw new IllegalStateException("Unable to find lambda's implementation method");
    }
}
