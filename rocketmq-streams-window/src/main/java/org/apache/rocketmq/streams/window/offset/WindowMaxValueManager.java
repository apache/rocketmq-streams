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
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.SQLUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.db.driver.orm.ORMUtil;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.operator.AbstractWindow;

import static org.apache.rocketmq.streams.window.offset.WindowMaxValue.MAX_VALUE_BASE_VALUE;

public class WindowMaxValueManager implements IWindowMaxValueManager {
    protected AbstractWindow window;

    protected Map<String, WindowMaxValue> windowOffsetMap=new HashMap<>();//all window offsets
    protected List<WindowMaxValue> needUpdateWindowValues =new ArrayList<>();//new windowoffset list, need save to storage when flush

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
    public Long updateWindowEventTime(String splitId, Long eventTime) {
        String windowId=StringUtil.createMD5Str(MapKeyUtil.createKey(window.getNameSpace(),window.getConfigureName()));
        String key=MapKeyUtil.createKey(splitId, windowId);
        WindowMaxValue windowOffset=queryOrCreateWindowOffset(key,true);
        return windowOffset.comareAndSet(eventTime);
    }

    @Override
    public Long updateWindowEventTime(String splitId, String formatEventTime) {
        if(StringUtil.isEmpty(formatEventTime)){
            return  updateWindowEventTime(splitId,(Long)null);
        }
        Long time= DateUtil.parseTime(formatEventTime).getTime();
        return  updateWindowEventTime(splitId,time);
    }

    @Override
    public Long incrementAndGetSplitNumber(String key) {
        WindowMaxValue windowOffset=queryOrCreateWindowOffset(key,true);
        return windowOffset.incrementAndGetMaxOffset();
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
    public void loadWindowMaxEventTime(Set<String> splitIds) {
        if(splitIds==null){
            return;
        }
        Set<String> keys=new HashSet<>();
        for(String splitId:splitIds){
            String windowId=StringUtil.createMD5Str(MapKeyUtil.createKey(window.getNameSpace(),window.getConfigureName()));
            String key=MapKeyUtil.createKey(splitId, windowId);
            keys.add(key);
        }

        queryOrCreateWindowOffset(keys,window.isLocalStorageOnly());
    }

    @Override
    public void flush(){
        if(window.isLocalStorageOnly()){
            needUpdateWindowValues =new ArrayList<>();
            return;
        }
        List<WindowMaxValue> windowOffsetList=new ArrayList<>();
        synchronized (this){
            windowOffsetList.addAll(needUpdateWindowValues);
            needUpdateWindowValues =new ArrayList<>();
        }
        ORMUtil.batchReplaceInto(windowOffsetList);
    }

    @Override
    public void resetSplitNum(WindowInstance instance, String splitId) {
        String key=createSplitNumberKey(instance,splitId);
        resetSplitNum(key);
    }

    @Override
    public synchronized void resetSplitNum(String key) {
        WindowMaxValue windowMaxValue=queryOrCreateWindowOffset(key,window.isLocalStorageOnly());
        windowMaxValue.maxValue.set(MAX_VALUE_BASE_VALUE);
        needUpdateWindowValues.add(windowMaxValue);
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
                    keysNotInDB.remove(windowMaxValue);
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
        needUpdateWindowValues.add(windowMaxValue);
        windowOffsetMap.put(key,windowMaxValue);
        return windowMaxValue;
    }
}
