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

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import org.apache.rocketmq.streams.common.cache.softreference.ICache;
import org.apache.rocketmq.streams.common.cache.softreference.impl.SoftReferenceCache;
import org.apache.rocketmq.streams.common.model.ServiceName;

public class ServiceLoadUtil {

    private static ICache<String,Object> serviceCache=new SoftReferenceCache<>();
    public static  <T> T loadService(Class interfaceClass,String serviceName){
        List<T> allService = (List<T>)serviceCache.get(interfaceClass.getName());
        if(allService==null){
            allService=new ArrayList<>();
            Iterable<T> iterable = ServiceLoader.load(interfaceClass);
            for (T t : iterable) {
                allService.add(t);
            }
            serviceCache.put(interfaceClass.getName(), allService);
        }
        if(CollectionUtil.isEmpty(allService)){
            return null;
        }
        if(StringUtil.isEmpty(serviceName)){
            return allService.get(0);
        }
        for(T t:allService){
            ServiceName annotation = (ServiceName)t.getClass().getAnnotation(ServiceName.class);
            if (annotation == null) {
                return null;
            }
            if (serviceName.equals(annotation.value())) {
                return t;
            }
            if (serviceName.equals(annotation.aliasName())) {
                return t;
            }

            if (serviceName.equals(annotation.name())) {
                return t;
            }
        }
        return null;
    }
}
