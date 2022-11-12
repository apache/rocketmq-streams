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
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static jdk.internal.org.objectweb.asm.Type.getConstructorDescriptor;
import static jdk.internal.org.objectweb.asm.Type.getMethodDescriptor;

public class TypeExtractor {

    private static TypeWrapper findInNormal(Object function, String name) {
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

    public static TypeWrapper find(Object function, String name) throws IllegalArgumentException {
        try {
            // get serialized lambda
            SerializedLambda serializedLambda = null;
            for (Class<?> clazz = function.getClass(); clazz != null; clazz = clazz.getSuperclass()) {
                try {
                    Method replaceMethod = clazz.getDeclaredMethod("writeReplace");
                    replaceMethod.setAccessible(true);
                    Object serialVersion = replaceMethod.invoke(function);

                    // check if class is a lambda function
                    if (serialVersion != null && serialVersion.getClass() == SerializedLambda.class) {
                        serializedLambda = (SerializedLambda) serialVersion;
                        break;
                    }
                } catch (NoSuchMethodException e) {
                    // thrown if the method is not there. fall through the loop
                }
            }

            // not a lambda method -> return null
            if (serializedLambda == null) {
                return findInNormal(function, name);
            }

            // find lambda method
            String className = serializedLambda.getImplClass();
            String methodName = serializedLambda.getImplMethodName();
            String methodSig = serializedLambda.getImplMethodSignature();

            Class<?> implClass = Class.forName(className.replace('/', '.'), true, Thread.currentThread().getContextClassLoader());

            // find constructor
            if (methodName.equals("<init>")) {
                Constructor<?>[] constructors = implClass.getDeclaredConstructors();
                for (Constructor<?> constructor : constructors) {
                    if (getConstructorDescriptor(constructor).equals(methodSig)) {
                        Class<?>[] parameterTypes = constructor.getParameterTypes();
                        Class<?> declaringClass = constructor.getDeclaringClass();
                        return new TypeWrapper(parameterTypes, declaringClass);
                    }
                }
            }
            // find method
            else {
                List<Method> methods = getAllDeclaredMethods(implClass);
                for (Method method : methods) {
                    if (method.getName().equals(methodName)
                            && getMethodDescriptor(method).equals(methodSig)) {
                        Class<?>[] parameterTypes = method.getParameterTypes();
                        Class<?> returnType = method.getReturnType();
                        return new TypeWrapper(parameterTypes, returnType);
                    }
                }
            }
            throw new IllegalArgumentException("No lambda method found.");
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Could not extract lambda method out of function: "
                            + e.getClass().getSimpleName()
                            + " - "
                            + e.getMessage(),
                    e);
        }
    }

    public static List<Method> getAllDeclaredMethods(Class<?> clazz) {
        List<Method> result = new ArrayList<>();
        while (clazz != null) {
            Method[] methods = clazz.getDeclaredMethods();
            Collections.addAll(result, methods);
            clazz = clazz.getSuperclass();
        }
        return result;
    }

}
