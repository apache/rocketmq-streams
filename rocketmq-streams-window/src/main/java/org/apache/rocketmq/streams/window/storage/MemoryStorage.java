package org.apache.rocketmq.streams.window.storage;
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

import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.state.WindowBaseValue;
import org.apache.rocketmq.streams.window.state.impl.JoinState;
import org.apache.rocketmq.streams.window.state.impl.WindowValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MemoryStorage implements IStorage {
    private Map<String/*参数拼接*/, String/*WindowInstanceKey组合*/> windowInstanceKeyAssemble = new HashMap<>();
    private Map<String/*WindowInstanceKey*/, WindowInstance> windowInstanceHolder = new HashMap<>();

    private Map<String/*参数拼接*/, String/*msgKey组合*/> msgKeyAssemble = new HashMap<>();
    private Map<String/*msgKey*/, WindowValue> windowValueHolder = new HashMap<>();

    private Map<String/*参数拼接*/, String/*msgKey组合*/> messageIdAssemble = new HashMap<>();
    private Map<String/*msgKey*/, JoinState> joinStateHolder = new HashMap<>();

    @Override
    public void init() {
    }

    @Override
    public void start() {
    }

    @Override
    public void putWindowInstance(String shuffleId, String windowNamespace, String windowConfigureName, WindowInstance windowInstance) {
        String windowInstanceKey = windowInstance.getWindowInstanceKey();

        windowInstanceHolder.put(windowInstanceKey, windowInstance);

        String key = buildKey(windowNamespace, windowConfigureName, shuffleId);
        putAssemble(windowInstanceKeyAssemble, key, windowInstanceKey);
    }

    @Override
    public List<WindowInstance> getWindowInstance(String shuffleId, String windowNamespace, String windowConfigureName) {
        String key = buildKey(windowNamespace, windowConfigureName, shuffleId);
        String assembleKey = windowInstanceKeyAssemble.get(key);

        if (assembleKey == null) {
            return null;
        }

        List<String> split = split(assembleKey);
        List<WindowInstance> result = new ArrayList<>();

        for (String windowInstanceKey : split) {
            result.add(windowInstanceHolder.get(windowInstanceKey));
        }

        return result;
    }

    @Override
    public void deleteWindowInstance(String shuffleId, String windowNamespace, String windowConfigureName, String windowInstanceKey) {
        this.windowInstanceHolder.remove(windowInstanceKey);
        //没有将windowInstanceKey从windowInstanceKeyAssemble中删除，需要遍历value然后找。
    }

    @Override
    public void putWindowBaseValue(String shuffleId, String windowInstanceId,
                                   WindowType windowType, WindowJoinType joinType,
                                   List<WindowBaseValue> windowBaseValue) {
        if (windowBaseValue == null || windowBaseValue.size() == 0) {
            return;
        }

        //todo shuffleId not used;

        for (WindowBaseValue baseValue : windowBaseValue) {
            String key;
            switch (windowType) {
                case NORMAL_WINDOW:
                case SESSION_WINDOW:
                    WindowValue windowValue = (WindowValue) baseValue;

                    String msgKey = windowValue.getMsgKey();
                    windowValueHolder.put(msgKey, windowValue);

                    key = buildKey(windowInstanceId, windowType.name());
                    putAssemble(msgKeyAssemble, key, msgKey);

                    break;
                case JOIN_WINDOW:
                    JoinState joinState = (JoinState) baseValue;

                    String messageId = joinState.getMessageId();
                    joinStateHolder.put(messageId, joinState);

                    key = buildKey(windowInstanceId, windowType.name(), joinType.name());
                    putAssemble(messageIdAssemble, key, messageId);

                    break;
            }
        }
    }

    private Map<String, String> putAssemble(Map<String, String> targetMap, String key, String addValue) {
        String oldValue = targetMap.get(key);

        String newValue;
        if (oldValue == null) {
            newValue = addValue;
            targetMap.put(key, newValue);
        } else {
            List<String> split = split(oldValue);
            if (!split.contains(addValue)) {
                split.add(addValue);
                newValue = buildKey(split.toArray(new String[0]));
                targetMap.put(key, newValue);
            }
        }
        return targetMap;
    }


    @Override
    public List<WindowBaseValue> getWindowBaseValue(String shuffleId, String windowInstanceId,
                                                    WindowType windowType, WindowJoinType joinType) {
        String key;
        switch (windowType) {
            case NORMAL_WINDOW:
            case SESSION_WINDOW: {
                key = buildKey(windowInstanceId, windowType.name());
                String msgKeyAssembleKey = msgKeyAssemble.get(key);
                if (msgKeyAssembleKey == null) {
                    return null;
                }

                List<String> split = split(msgKeyAssembleKey);
                ArrayList<WindowBaseValue> result = new ArrayList<>();
                for (String msgKey : split) {
                    result.add(windowValueHolder.get(msgKey));
                }
                return result;
            }
            case JOIN_WINDOW: {
                key = buildKey(windowInstanceId, windowType.name(), joinType.name());
                String messageIdAssembleKey = messageIdAssemble.get(key);
                if (messageIdAssembleKey == null) {
                    return null;
                }

                List<String> split = split(messageIdAssembleKey);
                ArrayList<WindowBaseValue> result = new ArrayList<>();
                for (String messageId : split) {
                    result.add(joinStateHolder.get(messageId));
                }
                return result;
            }
        }

        return null;
    }

    @Override
    public void deleteWindowBaseValue(String shuffleId, String windowInstanceId, WindowType windowType, WindowJoinType joinType) {

        switch (windowType) {
            case NORMAL_WINDOW:
            case SESSION_WINDOW: {
                String key = buildKey(windowInstanceId, windowType.name());
                String remove = msgKeyAssemble.remove(key);
                if (remove == null) {
                    return;
                }

                List<String> split = split(remove);
                for (String msgKey : split) {
                    windowValueHolder.remove(msgKey);
                }

                break;
            }
            case JOIN_WINDOW: {
                String key = buildKey(windowInstanceId, windowType.name(), joinType.name());
                String remove = messageIdAssemble.remove(key);

                if (remove == null) {
                    return;
                }

                List<String> split = split(remove);
                for (String msgKey : split) {
                    joinStateHolder.remove(msgKey);
                }

                break;
            }
        }
    }

    private Map<String, String> maxOffsetHolder = new HashMap<>();

    @Override
    public String getMaxOffset(String shuffleId, String windowConfigureName, String oriQueueId) {
        String key = buildKey(windowConfigureName, shuffleId, oriQueueId);
        return maxOffsetHolder.get(key);
    }

    @Override
    public void putMaxOffset(String shuffleId, String windowConfigureName, String oriQueueId, String offset) {
        String key = buildKey(windowConfigureName, shuffleId, oriQueueId);
        maxOffsetHolder.put(key, offset);
    }

    @Override
    public void deleteMaxOffset(String shuffleId, String windowConfigureName, String oriQueueId) {
        String key = buildKey(windowConfigureName, shuffleId, oriQueueId);
        maxOffsetHolder.remove(key);
    }

    private Map<String, Long> maxPartitionNumHolder = new HashMap<>();

    @Override
    public void putMaxPartitionNum(String shuffleId, String windowInstanceKey, long maxPartitionNum) {
        String key = buildKey(windowInstanceKey, shuffleId);
        maxPartitionNumHolder.put(key, maxPartitionNum);
    }

    @Override
    public Long getMaxPartitionNum(String shuffleId, String windowInstanceKey) {
        String key = buildKey(windowInstanceKey, shuffleId);
        return maxPartitionNumHolder.get(key);
    }

    @Override
    public void deleteMaxPartitionNum(String shuffleId, String windowInstanceKey) {
        String key = buildKey(windowInstanceKey, shuffleId);
        maxPartitionNumHolder.remove(key);
    }

    @Override
    public int flush(List<String> queueId) {
        return 0;
    }

    @Override
    public void clearCache(String queueId) {

    }

    private static String buildKey(String... args) {
        if (args == null || args.length == 0) {
            return null;
        }

        StringBuilder sb = new StringBuilder();
        for (String arg : args) {
            sb.append(arg);
            sb.append(IStorage.SEPARATOR);
        }

        return sb.substring(0, sb.lastIndexOf(IStorage.SEPARATOR));
    }

    private static List<String> split(String str) {
        String[] split = str.split(IStorage.SEPARATOR);
        return new ArrayList<>(Arrays.asList(split));
    }

}
