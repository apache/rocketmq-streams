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
package org.apache.rocketmq.streams.common.optimization;

import java.util.Set;

/**
 * compile and do match regex in bulk
 *
 * @param <T> the type of associated info
 * @author arthur liang
 */
public interface IStreamRegex<T> {

    /**
     * add regex and it's group info
     *
     * @param regex   the detail expression
     * @param context mostly group name
     */
    void addRegex(String regex, T context);

    /**
     * compile the regex
     */
    void compile();

    /**
     * match the regex
     *
     * @param content
     * @return if the content match the regex
     */
    boolean match(String content);

    /**
     * match the regex and return the matched expression
     *
     * @param content
     * @return return empty set if don't match
     */
    Set<T> matchExpression(String content);

    /**
     * the size of expression
     *
     * @return
     */
    int size();

}
