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

package org.apache.rocketmq.streams.common.checkpoint;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Iterator;
import java.util.ServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @description
 */
public class CheckPointStorageFactory {

    private static final Logger logger = LoggerFactory.getLogger(CheckPointStorageFactory.class);
    public static final String DEFAULT_CHECKPOINT_TYPE_NAME = "DB";
    private static CheckPointStorageFactory instance;
    private ServiceLoader<ICheckPointStorage> loader;

    private CheckPointStorageFactory() {
        URLClassLoader classLoader = (URLClassLoader) Thread.currentThread().getContextClassLoader();
        URL[] urls = classLoader.getURLs();
        for (URL u : urls) {
            String s = u.toString();
            if (s.contains("rocketmq-streams")) {
                logger.debug(String.format("list class : %s", s));
            }
        }
        loader = ServiceLoader.load(ICheckPointStorage.class);
    }

    public static CheckPointStorageFactory getInstance() {
        if (null == instance) {
            synchronized (CheckPointStorageFactory.class) {
                if (null == instance) {
                    instance = new CheckPointStorageFactory();
                }
            }
        }
        return instance;
    }

    public ICheckPointStorage getStorage(String name) {

        Iterator<ICheckPointStorage> it = loader.iterator();
        ICheckPointStorage storage = null;

        ICheckPointStorage defaultStorage = null;
        while (it.hasNext()) {
            ICheckPointStorage local = it.next();
            if (local.getStorageName().equalsIgnoreCase(name)) {
                return local;
            }
            if (local.getStorageName().equalsIgnoreCase(DEFAULT_CHECKPOINT_TYPE_NAME)) {
                defaultStorage = local;
            }
        }

        if (storage == null) {
            // logger.error(String.format("checkpoint storage name config error, name is %s. use default checkpoint type db.", name));
            return defaultStorage;
        }
        return null;
    }

}
