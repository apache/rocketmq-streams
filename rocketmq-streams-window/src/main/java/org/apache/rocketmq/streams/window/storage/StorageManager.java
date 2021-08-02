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
package org.apache.rocketmq.streams.window.storage;

/**
 * 存储管理，根据分片本地存储是否有效，来选择对应的存储完成计算
 */
public class StorageManager {
    //private ExecutorService executorService= Executors.newFixedThreadPool(10);
    //
    //private ExecutorService dbService = Executors.newSingleThreadExecutor();
    //
    //private static StorageManager storageManager=new StorageManager();
    //private StorageManager(){}
    //protected static IStorage rocksDBStorage=new RocksdbStorage();//本地存储
    //protected static DBStorage dbStorage=new DBStorage();//jdbc 存储
    ////0/null:false;1:true;2加载中
    ////
    //protected transient boolean isLocalOnly=false;//只支持本地存储
    //protected ConcurrentHashMap<String,Integer> shuffleIdAndWindowInstance2IsLocal=new ConcurrentHashMap<>();//某个分片是否本地存储有效
    //public static StorageManager getStorageManager(){
    //   return storageManager;
    //}
    //
    //public static IShufflePartitionManager getShufflePartitionManager(){
    //    return storageManager;
    //}
    //
    //public static IStorage getLocalStorage(){
    //    return rocksDBStorage;
    //}
    //
    //public static IStorage getRemoteStorage(){
    //    return dbStorage;
    //}
    //
    //@Override
    //public void put(Map<String, WindowBaseValue> values, boolean onlyLocal) {
    //    if(onlyLocal){
    //        rocksDBStorage.put(values, true);
    //        return;
    //    }
    //    Map<String,WindowBaseValue> notLocalWindowBaseValues=new HashMap<>();
    //    Map<String,WindowBaseValue> localWindowBaseValues=new HashMap<>();
    //    Iterator<Entry<String, WindowBaseValue>> it = values.entrySet().iterator();
    //    while (it.hasNext()){
    //        Entry<String, WindowBaseValue>entry=it.next();
    //        boolean isLocal=isLocalStorage(entry.getValue().getPartition(),entry.getValue().getWindowInstanceId());
    //        if(isLocal){
    //            localWindowBaseValues.put(entry.getKey(),entry.getValue());
    //        }else {
    //            notLocalWindowBaseValues.put(entry.getKey(),entry.getValue());
    //        }
    //    }
    //    rocksDBStorage.put(values, false);
    //    if(isLocalOnly){
    //        return;
    //    }
    //    /**
    //     *
    //     */
    //    if (!CollectionUtil.isEmpty(localWindowBaseValues)) {
    //        //如果本地可用，可以用异步的方式写，提高写性能
    //        dbService.execute(new Runnable() {
    //            @Override
    //            public void run() {
    //                dbStorage.put(getMd5Value(localWindowBaseValues), false);
    //            }
    //        });
    //    } else if (!CollectionUtil.isEmpty(notLocalWindowBaseValues)) {
    //        //如果本地不可用，必须同步写
    //        dbStorage.put(getMd5Value(notLocalWindowBaseValues), false);
    //    }
    //}
    //
    ///**
    // * DB存储时用MD5，TODO 考虑使用aop
    // */
    //private Map<String, WindowBaseValue> getMd5Value(Map<String, WindowBaseValue> originMap) {
    //    Map<String, WindowBaseValue> valueMap = new HashMap<>(originMap.size());
    //    Iterator<Entry<String, WindowBaseValue>> iterator = originMap.entrySet().iterator();
    //    while (iterator.hasNext()) {
    //        Entry<String, WindowBaseValue> entry = iterator.next();
    //        WindowBaseValue value = entry.getValue();
    //        if (value instanceof WindowValue) {
    //            WindowValue md5Value = (WindowValue)entry.getValue();
    //            valueMap.put(entry.getKey(), md5Value.toMd5Value());
    //        } else {
    //            //TODO join的MD5计算逻辑
    //            valueMap.put(entry.getKey(), entry.getValue());
    //        }
    //    }
    //    return valueMap;
    //}
    //
    //@Override
    //public Map<String, WindowBaseValue> get(Collection<String> keys, Class<? extends WindowBaseValue> clazz) {
    //    Map<String, WindowBaseValue> result=new HashMap<>();
    //    if(isLocalOnly){
    //        result.putAll(rocksDBStorage.get(keys,clazz));
    //        return result;
    //    }
    //
    //    List<String> notLocalKeys=new ArrayList<>();
    //    List<String> localKeys=new ArrayList<>();
    //    for(String key:keys){
    //        String[] values=MapKeyUtil.spliteKey(key);
    //        String shuffleId=values[0];
    //        boolean isLocal = isLocalStorage(shuffleId, WindowInstance.createWindowInstanceId(key));
    //        if(isLocal){
    //            localKeys.add(key);
    //        }else {
    //            notLocalKeys.add(key);
    //        }
    //    }
    //
    //    result.putAll(rocksDBStorage.get(localKeys,clazz));
    //    result.putAll(dbStorage.get(notLocalKeys,clazz));
    //    return result;
    //}
    //
    //@Override
    //public void delete(String windowNameSpace, String windowName, String startTime, String endOrFireTime,
    //    Class<? extends WindowBaseValue> clazz) {
    //    executorService.execute(new Runnable() {
    //
    //        @Override
    //        public void run() {
    //            rocksDBStorage.delete(windowNameSpace, windowName, startTime, endOrFireTime, clazz);
    //            if(!isLocalOnly){
    //                dbStorage.delete(windowNameSpace, windowName, startTime, endOrFireTime, clazz);
    //            }
    //
    //        }
    //    });
    //
    //}
    //
    //@Override
    //public void clearCache(ISplit channelQueue) {
    //    rocksDBStorage.clearCache(channelQueue);
    //}
    //
    //@Override
    //public Iterator<WindowBaseValue> loadWindowInstanceSplitData(String queueId, String windowNameSpace,
    //    String windowName, String startTime, String endOrFireTime, String key, Class<? extends WindowBaseValue> clazz) {
    //    boolean isLocal = isLocalStorage(queueId,
    //        WindowInstance.getWindowInstanceId(windowNameSpace, windowName, startTime, endOrFireTime));
    //    if(isLocal){
    //        return rocksDBStorage.loadWindowInstanceSplitData(queueId,windowNameSpace,windowName,startTime,endOrFireTime,key, clazz);
    //    }else {
    //        return dbStorage.loadWindowInstanceSplitData(queueId,windowNameSpace,windowName,startTime,endOrFireTime,key, clazz);
    //    }
    //}
    //
    //@Override
    //public long getMaxShuffleId(String queueId, String windowNameSpace, String windowName, String startTime,
    //    String endOrFireTime, Class<? extends WindowBaseValue> clazz) {
    //    if(isLocalOnly){
    //        return 0;
    //    }
    //    return dbStorage.getMaxShuffleId(queueId,windowNameSpace,windowName,startTime,endOrFireTime,clazz);
    //}
    //
    //@Override
    //public void loadSplitData2Local(String splitNumer, String windowNameSpace, String windowName,
    //    String startTime, String endOrFireTime, Class<? extends WindowBaseValue> clazz, IRowOperator rowOperator) {
    //    if(isLocalOnly){
    //        return;
    //    }
    //    String windowInstanceId = WindowInstance.getWindowInstanceId(windowNameSpace, windowName, startTime, endOrFireTime);
    //    Integer value=this.shuffleIdAndWindowInstance2IsLocal.get(MapKeyUtil.createKey(splitNumer,windowInstanceId));
    //    if(value!=null&&value!=0){
    //        return;
    //    }
    //    synchronized (this){
    //        value=this.shuffleIdAndWindowInstance2IsLocal.get(MapKeyUtil.createKey(splitNumer,windowInstanceId));
    //        if(value!=null&&value!=0){
    //            return;
    //        }
    //        shuffleIdAndWindowInstance2IsLocal.put(MapKeyUtil.createKey(splitNumer,windowInstanceId),2);
    //        executorService.execute(new Runnable() {
    //            @Override
    //            public void run() {
    //                if (rowOperator == null) {
    //                    dbStorage.loadSplitData2Local(splitNumer, windowNameSpace, windowName, startTime, endOrFireTime,
    //                        clazz,
    //                        new IRowOperator() {
    //                            @Override
    //                            public void doProcess(Map<String, Object> row) {
    //                                WindowBaseValue theValue = ORMUtil.convert(row, clazz);
    //                                List<String> keys = new ArrayList<>();
    //                                keys.add(theValue.getMsgKey());
    //                                WindowBaseValue windowBaseValue = (WindowBaseValue)rocksDBStorage.get(keys, clazz);
    //                                if (windowBaseValue == null) {
    //                                    Map<String, WindowBaseValue> map = new HashMap<>();
    //                                    map.put(theValue.getMsgKey(), theValue);
    //                                    rocksDBStorage.put(map, true);
    //                                    return;
    //                                }
    //                                if (theValue.getUpdateVersion() > windowBaseValue.getUpdateVersion()) {
    //                                    Map<String, WindowBaseValue> map = new HashMap<>();
    //                                    map.put(theValue.getMsgKey(), theValue);
    //                                    rocksDBStorage.put(map, true);
    //                                }
    //                            }
    //                        });
    //                } else {
    //                    dbStorage.loadSplitData2Local(splitNumer, windowNameSpace, windowName, startTime, endOrFireTime,
    //                        clazz, rowOperator);
    //                }
    //                shuffleIdAndWindowInstance2IsLocal.put(MapKeyUtil.createKey(splitNumer, windowInstanceId), 1);
    //            }
    //        });
    //    }
    //
    //
    //}
    //
    //
    //@Override
    //public boolean isLocalStorage(String shuffleId,String windowInstanceId) {
    //    Integer value=this.shuffleIdAndWindowInstance2IsLocal.get(MapKeyUtil.createKey(shuffleId,windowInstanceId));
    //    return isLocalStorage(value)||isLocalOnly;
    //}
    //@Override
    //public void setLocalStorageInvalid(ISplit channelQueue,String windowInstanceId) {
    //    this.shuffleIdAndWindowInstance2IsLocal.remove(MapKeyUtil.createKey(channelQueue.getQueueId(),windowInstanceId),false);
    //}
    //@Override
    //public void setLocalStorageInvalid(ISplit channelQueue) {
    //    Iterator<Entry<String, Integer>> it = this.shuffleIdAndWindowInstance2IsLocal.entrySet().iterator();
    //    List<String> keys=new ArrayList<>();
    //    while (it.hasNext()){
    //        Entry<String, Integer> entry=it.next();
    //        String key=entry.getKey();
    //        if(key.startsWith(channelQueue.getQueueId())){
    //            this.shuffleIdAndWindowInstance2IsLocal.put(key,0);
    //            keys.add(entry.getKey());
    //        }
    //
    //    }
    //    executorService.execute(new Runnable() {
    //        @Override
    //        public void run() {
    //            clearCache(channelQueue);
    //            for(String key:keys){
    //                shuffleIdAndWindowInstance2IsLocal.remove(key);
    //            }
    //        }
    //    });
    //}
    //
    //@Override
    //public boolean setLocalStorageValdateIfNotExist(String shuffleId,String windowInstanceId) {
    //    Integer value = this.shuffleIdAndWindowInstance2IsLocal.get(
    //        MapKeyUtil.createKey(shuffleId, windowInstanceId));
    //    if (value != null) {
    //        return isLocalStorage(value);
    //    }
    //    this.shuffleIdAndWindowInstance2IsLocal.put( MapKeyUtil.createKey(shuffleId, windowInstanceId), 1);
    //    return true;
    //}
    //
    //
    //@Override
    //public void clearWindowInstanceStorageStatus(String windowInstanceId,Collection<String> queueIds){
    //    if(queueIds!=null){
    //        for(String queueId:queueIds){
    //            this.shuffleIdAndWindowInstance2IsLocal.remove(MapKeyUtil.createKey(queueId,windowInstanceId));
    //        }
    //    }
    //}
    //
    //
    //public boolean isLocalStorage(Integer value){
    //    if(value!=null&&value==1){
    //        return true;
    //    }
    //    if(isLocalOnly){
    //        return true;
    //    }
    //    return false;
    //}
    //
    //public boolean isLocalOnly() {
    //    return isLocalOnly;
    //}
    //
    //public void setLocalOnly(boolean localOnly) {
    //    isLocalOnly = localOnly;
    //}
}
