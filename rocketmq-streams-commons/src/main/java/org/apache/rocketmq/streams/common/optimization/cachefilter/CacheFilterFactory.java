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

package org.apache.rocketmq.streams.common.optimization.cachefilter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.streams.common.calssscaner.AbstractScan;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;

public class CacheFilterFactory {

    protected List<ICacheFilterBulider> cacheFilterCreators=new ArrayList<>();
    protected static CacheFilterFactory expressionFactory=new CacheFilterFactory();
    protected static AtomicBoolean isScaned=new AtomicBoolean(false);
    protected AbstractScan scan=new AbstractScan() {
        @Override protected void doProcessor(Class clazz) {
            if(ICacheFilterBulider.class.isAssignableFrom(clazz)){
                cacheFilterCreators.add(ReflectUtil.forInstance(clazz));
            }
        }
    };

    public static CacheFilterFactory getInstance(){
        if(isScaned.compareAndSet(false,true)){
            expressionFactory.scan.scanPackages("org.apache.rocketmq.streams.script.optimization.performance");
            expressionFactory.scan.scanPackages("org.apache.rocketmq.streams.filter.function");
        }
        return expressionFactory;
    }

    public boolean supportProxy(Object oriExpression){
        for(ICacheFilterBulider cacheFilterBulider:this.cacheFilterCreators){
            if(cacheFilterBulider.support(oriExpression)){
                return true;
            }
        }
        return false;
    }

    public ICacheFilter create(Object oriExpression){
        for(ICacheFilterBulider cacheFilterBulider:this.cacheFilterCreators){
            if(cacheFilterBulider.support(oriExpression)){
                return cacheFilterBulider.create(oriExpression);
            }
        }
        return null;
    }
}
