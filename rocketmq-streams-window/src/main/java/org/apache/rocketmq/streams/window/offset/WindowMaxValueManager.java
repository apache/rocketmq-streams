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
package org.apache.rocketmq.streams.window.offset;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.servlet.jsp.HttpJspPage;
import org.apache.rocketmq.streams.common.context.MessageOffset;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.SQLUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.db.driver.DriverBuilder;
import org.apache.rocketmq.streams.db.driver.orm.ORMUtil;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.operator.AbstractWindow;

import static org.apache.rocketmq.streams.window.offset.WindowMaxValue.MAX_VALUE_BASE_VALUE;

public class WindowMaxValueManager implements IWindowMaxValueManager {
    protected AbstractWindow window;

    protected Map<String, WindowMaxValue> windowOffsetMap=new HashMap<>();//all window offsets
    protected List<WindowMaxValue> deleteWindowValues =new ArrayList<>();//new windowoffset list, need save to storage when flush
    public WindowMaxValueManager(AbstractWindow window){
        this.window=window;
    }

    @Override
    public String createSplitNumberKey(WindowInstance instance, String splitId){
        String key= MapKeyUtil.createKey(splitId,instance.getWindowInstanceKey());
        return key;
    }

    @Override
    public Long incrementAndGetSplitNumber(WindowInstance instance, String splitId) {
        String key=createSplitNumberKey(instance,splitId);
        WindowMaxValue windowMaxValue=queryOrCreateWindowOffset(key,true);
        return windowMaxValue.incrementAndGetMaxOffset();
    }


    @Override
    public void loadMaxSplitNum(Set<WindowInstance> windowInstances, String splitId) {
        if(windowInstances==null||StringUtil.isEmpty(splitId)){
            return;
        }
        Set<String> keys=new HashSet<>();
        for(WindowInstance instance:windowInstances){
            String key= createSplitNumberKey(instance,splitId);
            keys.add(key);
        }
        queryOrCreateWindowOffset(keys,window.isLocalStorageOnly());
    }

    @Override
    public void loadMaxSplitNum(Set<String> keys) {
        queryOrCreateWindowOffset(keys,window.isLocalStorageOnly());
    }


    @Override
    public void flush(){
        if(window.isLocalStorageOnly()){
            deleteWindowValues=new ArrayList<>();
            return;
        }
        List<WindowMaxValue> windowOffsetList=new ArrayList<>();
        synchronized (this){
            windowOffsetList.addAll(windowOffsetMap.values());
        }
        ORMUtil.batchReplaceInto(windowOffsetList);
        if(deleteWindowValues!=null&&this.deleteWindowValues.size()>0){
            List<String> dels=new ArrayList<>();
            synchronized (this){
                for(WindowMaxValue windowMaxValue:this.deleteWindowValues){
                    dels.add(windowMaxValue.getMsgKey());
                }
                this.deleteWindowValues=new ArrayList<>();
            }
            String sql="delete from "+ORMUtil.getTableName(WindowMaxValue.class)+" where msg_key in("+SQLUtil.createInSql(dels)+")";
            DriverBuilder.createDriver().execute(sql);
        }


    }

    @Override
    public void resetSplitNum(WindowInstance instance, String splitId) {
        String key=createSplitNumberKey(instance,splitId);
        resetSplitNum(key);
    }

    @Override
    public void deleteSplitNum(WindowInstance instance, String splitId) {
        String key=createSplitNumberKey(instance,splitId);
        WindowMaxValue windowMaxValue=queryOrCreateWindowOffset(key,window.isLocalStorageOnly());
        deleteWindowValues.add(windowMaxValue);
        this.windowOffsetMap.remove(key);
    }

    @Override
    public synchronized void resetSplitNum(String key) {
        WindowMaxValue windowMaxValue=queryOrCreateWindowOffset(key,window.isLocalStorageOnly());
        windowMaxValue.maxValue.set(MAX_VALUE_BASE_VALUE);
    }

    @Override public void saveMaxOffset(boolean isLong, String name,Map<String, String> queueId2Offsets) {
        Set<String> keys=new HashSet<>();
        for(String key:queueId2Offsets.keySet()){
            keys.add(MapKeyUtil.createKey(name,key));
        }
        Map<String,WindowMaxValue> windowMaxValueMap=queryOrCreateWindowOffset(keys,window.isLocalStorageOnly());
        for(String queueId:queueId2Offsets.keySet()){
            String key=MapKeyUtil.createKey(name,queueId);
            WindowMaxValue windowMaxValue=windowMaxValueMap.get(key);
            String currentOffset=queueId2Offsets.get(queueId);
            MessageOffset messageOffset=new MessageOffset(currentOffset,isLong);
            if(windowMaxValue.getMaxOffset().equals("-1")){
                windowMaxValue.setMaxOffset(currentOffset);
            }else {
                if(messageOffset.greateThan(windowMaxValue.getMaxOffset())){
                    windowMaxValue.setMaxOffset(currentOffset);
                }
            }
            windowMaxValue.setMaxOffsetLong(isLong);

        }
    }

    @Override public String loadOffset(String name, String queueId) {
        Set<String> queueIds=new HashSet<>();
        queueIds.add(queueId);
        Map<String,String> result=loadOffsets(name,queueIds);
        return result.get(queueId);
    }

    @Override public Map<String, String> loadOffsets(String name, Set<String> queueIds) {
        Set<String> keys=new HashSet<>();
        Map<String,String>key2QueueIds=new HashMap<>();
        for(String queueId:queueIds){
            String key=MapKeyUtil.createKey(name,queueId);
            keys.add(key);
            key2QueueIds.put(key,queueId);
        }
        Map<String,WindowMaxValue> windowMaxValueMap=queryOrCreateWindowOffset(keys,window.isLocalStorageOnly());
        if(windowMaxValueMap==null){
            return null;
        }
        Map<String,String> result=new HashMap<>();
        for(String key:windowMaxValueMap.keySet()){
            WindowMaxValue windowMaxValue=windowMaxValueMap.get(key);
            if(windowMaxValue!=null&&!windowMaxValue.getMaxOffset().equals("-1")){
                result.put(key2QueueIds.get(windowMaxValue.getMsgKey()),windowMaxValue.getMaxOffset());
            }
        }
        return result;
    }

    @Override
    public synchronized void removeKeyPrefixFromLocalCache(Set<String> keyPrefixs) {
        Map<String, WindowMaxValue> copy=new HashMap<>();
        copy.putAll(this.windowOffsetMap);
        for(String key:copy.keySet()){
            for(String keyPrefix:keyPrefixs){
                if(key.startsWith(keyPrefix)){
                    this.windowOffsetMap.remove(key);
                }
            }
        }

    }

    /**
     *  查询window的总计数器
     *
     * @return
     */
    protected WindowMaxValue queryOrCreateWindowOffset(String key,boolean onlyLocal){
        Set<String> keys=new HashSet<>();
        keys.add(key);
        Map<String,WindowMaxValue> windowMaxValueMap=queryOrCreateWindowOffset(keys,onlyLocal);
        if(windowMaxValueMap==null){
            return null;
        }
        return windowMaxValueMap.values().iterator().next();
    }

    /**
     *  查询window的总计数器
     *
     * @return
     */
    protected Map<String,WindowMaxValue> queryOrCreateWindowOffset(Set<String> keys,boolean onlyLocal){
        Map<String,WindowMaxValue> result=new HashMap<>();
        if(keys==null){
            return result;
        }
        List<String> keyNotInLocal=new ArrayList<>();
        for(String key:keys){
            WindowMaxValue windowMaxValue=windowOffsetMap.get(key);
            if(windowMaxValue !=null){
                result.put(key,windowMaxValue);
            }else if(onlyLocal){
                windowMaxValue=create(key);
                result.put(key,windowMaxValue);
            }else {
                keyNotInLocal.add(key);
            }
        }

        if(onlyLocal){
            return result;
        }
        if(keyNotInLocal.size()==0){
            return result;
        }
        synchronized (this){
            List<String> synchKeyNotInLocal=new ArrayList<>();
            for(String key:keyNotInLocal) {
                WindowMaxValue windowMaxValue = windowOffsetMap.get(key);
                if (windowMaxValue != null) {
                    result.put(key, windowMaxValue);
                }else {
                    synchKeyNotInLocal.add(key);
                }
            }
            List<WindowMaxValue> windowMaxValues=null;
            if(synchKeyNotInLocal.size()>0){
                String sql="select * from "+ ORMUtil.getTableName(WindowMaxValue.class)+ " where msg_key in ("+ SQLUtil.createInSql(synchKeyNotInLocal) +")";
                windowMaxValues=ORMUtil.queryForList(sql,null, WindowMaxValue.class);

            }
            //   String key= MapKeyUtil.createKey(window.getNameSpace(),window.getConfigureName(),split);
            List<String> keysNotInDB=new ArrayList<>();
            keysNotInDB.addAll(synchKeyNotInLocal);
            if(windowMaxValues!=null){
                for(WindowMaxValue windowMaxValue:windowMaxValues){
                    result.put(windowMaxValue.getMsgKey(), windowMaxValue);
                    keysNotInDB.remove(windowMaxValue.getMsgKey());
                    windowOffsetMap.put(windowMaxValue.getMsgKey(),windowMaxValue);
                }
            }
            if(keysNotInDB!=null&&keysNotInDB.size()>0){
                for(String key:keysNotInDB){
                    result.put(key, create(key));
                }
            }
        }
        return result;
    }

    protected WindowMaxValue create(String key){
        WindowMaxValue windowMaxValue=new WindowMaxValue();
        windowMaxValue.setGmtCreate(new Date());
        windowMaxValue.setGmtModified(new Date());
        windowMaxValue.setMsgKey(key);
        windowMaxValue.setMaxValue(MAX_VALUE_BASE_VALUE);
        windowMaxValue.setMaxEventTime(null);
        windowOffsetMap.put(key,windowMaxValue);
        return windowMaxValue;
    }
}
