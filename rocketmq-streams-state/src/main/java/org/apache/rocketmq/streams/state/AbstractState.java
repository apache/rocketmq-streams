package org.apache.rocketmq.streams.state;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
public abstract class AbstractState<K,V> implements IState<K,V>{


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


}
