package org.apache.rocketmq.streams.window.state;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import org.apache.rocketmq.streams.state.AbstractState;
import org.apache.rocketmq.streams.window.model.WindowInstance;

public abstract class AbstractMapState extends AbstractState {
    protected WindowInstance windowInstance;
    public AbstractMapState(WindowInstance windowInstance){
        this.windowInstance=windowInstance;
    }

    public static void main(String[] args) {
        TreeMap<String,Integer> treeMa=new TreeMap<>();
        treeMa.put("2",2);
        treeMa.put("3",3);
        treeMa.put("1",1);

        Iterator<Map.Entry<String, Integer>> it = treeMa.entrySet().iterator();
        while (it.hasNext()){
            Map.Entry<String, Integer> entry=it.next();
            System.out.println(entry.getKey());
        }
    }


}
