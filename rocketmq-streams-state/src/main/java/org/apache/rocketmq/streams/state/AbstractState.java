package org.apache.rocketmq.streams.state;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;

public abstract class AbstractState<K,V> implements IState<K,V>{
    protected transient Class valueClass;
    protected String[] namespaces;
    public AbstractState(Class valueClass,String... namespaces){
        this.namespaces=namespaces;
        this.valueClass=valueClass;
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

    @Override public String getNamespace() {
        return MapKeyUtil.createKey(namespaces);
    }


}
