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

import java.util.Set;
import org.apache.rocketmq.streams.window.model.WindowInstance;

/**
 * create split sequence number for window instance output result order by split sequence number when windown instance fire
 */
public interface IWindowMaxValueManager {

    /**
     * create split sequence number if the generator is not in memory， need load from db or other storage if instance is new ，set the split sequence number = init value
     *
     * @param instance
     * @param splitId
     * @return plus one on the current max split sequence number
     */
    Long incrementAndGetSplitNumber(WindowInstance instance, String splitId);

    /**
     * create split sequence number if the generator is not in memory， need load from db or other storage if instance is new ，set the split sequence number = init value
     *
     * @param key
     * @return plus one on the current max split sequence number
     */
    Long incrementAndGetSplitNumber(String key);


    /**
     * load mutil window instance split's max split num
     *
     * @param windowInstances
     * @param split
     */
    void loadMaxSplitNum(Set<WindowInstance> windowInstances, String split);

    String createSplitNumberKey(WindowInstance instance, String splitId);

    /**
     * load mutil window instance split's max split num
     *
     * @param keys
     * @return
     */
    void loadMaxSplitNum(Set<String> keys);

    void removeKeyPrefixFromLocalCache(Set<String> keyPrefixs);

    //load window max event time
    void loadWindowMaxEventTime(Set<String> splitId);

    /**
     * save addition WindowMaxValue
     */
    void flush();

    void resetSplitNum(WindowInstance instance, String splitId);

    void resetSplitNum(String key);
}
