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

package org.apache.rocketmq.streams.state;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.streams.serviceloader.ServiceLoaderComponent;
import org.apache.rocketmq.streams.state.backend.IStateBackend;

public abstract class AbstractState<K,V> implements IState<K,V>{
    protected String namespace;//ex，in rocksdb is prefix
    protected IStateBackend stateBackend; //state storage impl ex rocksdb
    protected String backendName;//choose stateBackend
    protected AtomicBoolean hasLoadStateBackend=new AtomicBoolean(false);

    public AbstractState(String namespace,String backendName){
        this.namespace=namespace;
        this.backendName=backendName;
    }

    @Override public boolean isEmpty() {
        return size()==0;
    }

    @Override public boolean containsKey(K key) {
        return get(key)!=null;
    }

    @Override public V get(K key) {
        List<K> keys=new ArrayList<>();
        Map<K,V> result=get(keys);
        if(result==null){
            return null;
        }
        return result.values().iterator().next();
    }

    @Override public V put(K key, V value) {
        Map<K,V> map=new HashMap<>();
        map.put(key,value);
        putAll(map);
        return value;
    }

    @Override public int size() {
        return getOrLoadStateBackend().size(this.namespace);
    }

    @Override public Map<K, V> get(List<K> key) {
        return getOrLoadStateBackend().get(this.namespace,key);
    }

    @Override public V remove(K key) {
        return (V)getOrLoadStateBackend().remove(this.namespace,key);
    }

    @Override public void putAll(Map<? extends K, ? extends V> m) {
        getOrLoadStateBackend().putAll(this.namespace,m);
    }

    @Override public void clear() {
        getOrLoadStateBackend().clear(this.namespace);
    }

    @Override public Iterator<K> keyIterator() {
        return  getOrLoadStateBackend().keyIterator(this.namespace);
    }

    @Override public Iterator<Map.Entry<K, V>> entryIterator() {
        return getOrLoadStateBackend().entryIterator(this.namespace);
    }

    @Override public void removeKeys(Collection<String> keys) {
        getOrLoadStateBackend().removeKeys(this.namespace,keys);
    }

    @Override public void scanEntity(IEntryProcessor<K, V> processor) {
        getOrLoadStateBackend().scanEntity(this.namespace,processor);
    }

    @Override public V putIfAbsent(K key, V value) {
        return  (V)getOrLoadStateBackend().putIfAbsent(this.namespace,key,value);
    }

    protected IStateBackend getOrLoadStateBackend(){
        if(hasLoadStateBackend.compareAndSet(false,true)){
            ServiceLoaderComponent serviceLoaderComponent=ServiceLoaderComponent.getInstance(IStateBackend.class);
            this.stateBackend= (IStateBackend) serviceLoaderComponent.getService().loadService(backendName);
        }
        return this.stateBackend;
    }

    public String getNamespace() {
        return namespace;
    }
}
