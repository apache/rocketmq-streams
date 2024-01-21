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
package org.apache.rocketmq.streams.common.model;

import java.util.concurrent.Executors;

public class NameCreatorContext {

    private static ThreadLocal<NameCreator> threadLocal = new ThreadLocal<>();

    public static NameCreator get() {
        NameCreator nameCreator = threadLocal.get();
        if (nameCreator == null) {
            nameCreator = new NameCreator();
        }
        threadLocal.set(nameCreator);
        return nameCreator;
    }

    public static void remove() {
        threadLocal.remove();
    }

    public static void main(String[] args) {

        System.out.println("主线程: " + NameCreatorContext.get().createName("1111", "22222"));
        System.out.println("主线程: " + NameCreatorContext.get().createName("1111", "22222"));
        System.out.println("主线程: " + NameCreatorContext.get().createName("1111", "22222"));
        System.out.println("主线程: " + NameCreatorContext.get().createName("1111", "22222"));
        System.out.println("主线程: " + NameCreatorContext.get().createName("3333", "4444"));
        System.out.println("主线程: " + NameCreatorContext.get().createName("3333", "4444"));
        System.out.println("主线程: " + NameCreatorContext.get().createName("3333", "4444"));

        Executors.newSingleThreadExecutor().execute(new Runnable() {
            @Override
            public void run() {
                System.out.println("线程1: " + NameCreatorContext.get().createName("1111", "22222"));
                System.out.println("线程1: " + NameCreatorContext.get().createName("1111", "22222"));
                System.out.println("线程1: " + NameCreatorContext.get().createName("1111", "22222"));
                System.out.println("线程1: " + NameCreatorContext.get().createName("1111", "22222"));
            }
        });

        Executors.newSingleThreadExecutor().execute(new Runnable() {
            @Override
            public void run() {
                System.out.println("线程2: " + NameCreatorContext.get().createName("3333", "4444"));
                System.out.println("线程2: " + NameCreatorContext.get().createName("3333", "4444"));
                System.out.println("线程2: " + NameCreatorContext.get().createName("3333", "4444"));
                System.out.println("线程2: " + NameCreatorContext.get().createName("3333", "4444"));
                System.out.println("线程2: " + NameCreatorContext.get().createName("3333", "4444"));
                System.out.println("线程2: " + NameCreatorContext.get().createName("3333", "4444"));
            }
        });

    }

}
