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
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.rocketmq.streams.common.context.MessageOffset;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.SQLUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.db.driver.DriverBuilder;
import org.apache.rocketmq.streams.db.driver.orm.ORMUtil;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.operator.AbstractWindow;
import org.apache.rocketmq.streams.window.sqlcache.SQLCache;
import org.apache.rocketmq.streams.window.sqlcache.impl.SQLElement;

import static org.apache.rocketmq.streams.window.offset.WindowMaxValue.MAX_VALUE_BASE_VALUE;

public class WindowMaxValueProcessor{
    protected AbstractWindow window;
    protected String splitId;
    protected SQLCache sqlCache;

    public WindowMaxValueProcessor(String splitId, AbstractWindow window,
        SQLCache sqlCache){
        this.splitId=splitId;
        this.window=window;
        this.sqlCache=sqlCache;
    }

    protected Map<String, WindowMaxValue> windowOffsetMap=new HashMap<>();//all window offsets
    //protected List<WindowMaxValue> deleteWindowValues =new ArrayList<>();//new windowoffset list, need save to storage when flush




    public Long incrementAndGetSplitNumber(WindowInstance instance) {
        String key=createSplitNumberKey(instance,splitId);
        WindowMaxValue windowMaxValue=queryOrCreateWindowOffset(key,true);
        return windowMaxValue.incrementAndGetMaxOffset();
    }



    public WindowMaxValue querySplitNum(WindowInstance instance) {
        String key= createSplitNumberKey(instance,splitId);
        return this.windowOffsetMap.get(key);
    }


//    public void flush(){
//        if(window.isLocalStorageOnly()){
//           // deleteWindowValues=new ArrayList<>();
//            return;
//        }
//        List<WindowMaxValue> windowOffsetList=new ArrayList<>();
//        synchronized (this){
//            windowOffsetList.addAll(windowOffsetMap.values());
//        }
//        if(sqlCache!=null){
//            sqlCache.addCache(new MutablePair<>(this.splitId,ORMUtil.createBatchReplacetSQL(windowOffsetList)));
//        }else {
//            ORMUtil.batchReplaceInto(windowOffsetList);
//        }
//
////        if(deleteWindowValues!=null&&this.deleteWindowValues.size()>0){
////            List<String> dels=new ArrayList<>();
////            synchronized (this){
////                for(WindowMaxValue windowMaxValue:this.deleteWindowValues){
////                    dels.add(windowMaxValue.getMsgKey());
////                }
////                this.deleteWindowValues=new ArrayList<>();
////            }
////            String sql="delete from "+ORMUtil.getTableName(WindowMaxValue.class)+" where msg_key in("+SQLUtil.createInSql(dels)+")";
////
////            if(sqlCache!=null){
////                sqlCache.addCache(new MutablePair<>(this.splitId,sql));
////            }else {
////                DriverBuilder.createDriver().execute(sql);
////            }
////        }
//
//
//    }

    public void resetSplitNum(WindowInstance instance) {
        String key=createSplitNumberKey(instance,splitId);
        WindowMaxValue windowMaxValue=queryOrCreateWindowOffset(key,window.isLocalStorageOnly());
        windowMaxValue.maxValue.set(MAX_VALUE_BASE_VALUE);
    }

    public void deleteSplitNum(WindowInstance instance) {
        String key=createSplitNumberKey(instance,splitId);
        WindowMaxValue windowMaxValue=queryOrCreateWindowOffset(key,window.isLocalStorageOnly());

        this.windowOffsetMap.remove(key);
        List<String> dels=new ArrayList<>();
        dels.add(windowMaxValue.getMsgKey());
        String sql="delete from "+ORMUtil.getTableName(WindowMaxValue.class)+" where msg_key in("+SQLUtil.createInSql(dels)+")";

        if(sqlCache!=null){
            sqlCache.addCache(new SQLElement(this.splitId,instance.createWindowInstanceId(),sql));
        }else {
            DriverBuilder.createDriver().execute(sql);
        }
    }


     public Map<String, WindowMaxValue> saveMaxOffset(boolean isLong, String name,Map<String, String> queueId2Offsets) {
         Map<String, WindowMaxValue> result=new HashMap<>();
        Set<String> keys=new HashSet<>();
        for(String key:queueId2Offsets.keySet()){
            keys.add(MapKeyUtil.createKey(name,splitId,key));
        }
        Map<String,WindowMaxValue> windowMaxValueMap=queryOrCreateWindowOffset(keys,window.isLocalStorageOnly());
        for(String queueId:queueId2Offsets.keySet()){
            String key=MapKeyUtil.createKey(name,splitId,queueId);
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
            result.put(key,windowMaxValue);
        }
        return result;
    }
    public Map<String, WindowMaxValue> queryOffsets(String name,Set<String> oriQueueIds) {
        Map<String,WindowMaxValue> result=new HashMap<>();
        for(String oriQueueId:oriQueueIds){
            String key=MapKeyUtil.createKey(name,splitId,oriQueueId);
            WindowMaxValue windowMaxValue=windowOffsetMap.get(key);
            result.put(key,windowMaxValue);
        }
        return result;
    }

    public Map<String,String> loadOffset(String name) {
        Map<String,String> result=new HashMap<>();
        String keyPrefix=MapKeyUtil.createKey(name,splitId);
        String sql="select * from "+ ORMUtil.getTableName(WindowMaxValue.class)+ " where msg_key like '"+keyPrefix+"%'";
        List<WindowMaxValue> windowMaxValues = ORMUtil.queryForList(sql, null, WindowMaxValue.class);
        if(windowMaxValues==null||windowMaxValues.size()==0){
            return result;
        }

        for(WindowMaxValue windowMaxValue:windowMaxValues){
            if(windowMaxValue!=null&&!windowMaxValue.getMaxOffset().equals("-1")){
                result.put(windowMaxValue.getMsgKey(),windowMaxValue.getMaxOffset());
            }
        }
        return result;
    }



    public synchronized void removeKeyPrefixFromLocalCache() {
        Map<String, WindowMaxValue> copy=new HashMap<>();
        copy.putAll(this.windowOffsetMap);
        for(String key:copy.keySet()){
            if(key.startsWith(this.splitId)){
                this.windowOffsetMap.remove(key);
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
    protected String createSplitNumberKey(WindowInstance instance, String splitId){
        String key= MapKeyUtil.createKey(splitId,instance.getWindowInstanceKey());
        return key;
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

    public void initMaxSplitNum(WindowInstance windowInstance,Long maxSplitNum) {
        String key=createSplitNumberKey(windowInstance,splitId);
        WindowMaxValue windowMaxValue=create(key);
        if(maxSplitNum!=null){
            windowMaxValue.setMaxValue(maxSplitNum);
        }
    }
}
