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
package org.apache.rocketmq.streams.core.state;

import com.google.common.base.Objects;

public class RocksDBKey {
    private Object keyObject;

    private Class<?> keyClazz;

    private Class<?> valueClazz;

    public RocksDBKey(Object keyObject, Class<?> keyClazz, Class<?> valueClazz) {
        this.keyObject = keyObject;
        this.keyClazz = keyClazz;
        this.valueClazz = valueClazz;
    }

    @SuppressWarnings("unchecked")
    public <K> K getKeyObject() {
        return (K) keyObject;
    }

    public void setKeyObject(Object keyObject) {
        this.keyObject = keyObject;
    }

    @SuppressWarnings("unchecked")
    public <K> Class<K> getKeyClazz() {
        return (Class<K>) keyClazz;
    }

    public <K> void setKeyClazz(Class<K> keyClazz) {
        this.keyClazz = keyClazz;
    }

    @SuppressWarnings("unchecked")
    public <V> Class<V> getValueClazz() {
        return (Class<V>) valueClazz;
    }

    public <V> void setValueClazz(Class<V> valueClazz) {
        this.valueClazz = valueClazz;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RocksDBKey that = (RocksDBKey) o;
        return Objects.equal(keyObject, that.keyObject);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(keyObject);
    }
}
