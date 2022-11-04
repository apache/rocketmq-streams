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
package org.apache.rocketmq.streams.dispatcher;

import java.util.Set;

public interface IMapper<T> {

    /**
     * get the instances
     * @return instance set
     * @throws Exception exception
     */
    Set<String> getInstances() throws Exception;

    /**
     * get the task list on the instance
     *
     * @param instance instance
     * @return task list
     * @throws Exception exception
     */
    Set<T> getTasks(String instance) throws Exception;

    /**
     * get the Task lie
     * @return task list
     * @throws Exception exception
     */
    Set<T> getTasks() throws Exception;

    /**
     * put the task into the instance task list
     *
     * @param instance instance
     * @param task     task
     * @throws Exception exception
     */
    void putTask(String instance, T task) throws Exception;

    /**
     * remove the task
     *
     * @param instance instance
     * @param task     task
     * @throws Exception exception
     */
    void removeTask(String instance, T task) throws Exception;

    /**
     * remove the instance
     *
     * @param instance instance
     * @throws Exception exception
     */
    void remove(String instance) throws Exception;

    /**
     * check wether the task has bean dispatched
     *
     * @param task task
     * @return boolean
     * @throws Exception exception
     */
    boolean containTask(T task) throws Exception;

}
