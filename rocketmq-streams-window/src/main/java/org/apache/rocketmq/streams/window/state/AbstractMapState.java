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
