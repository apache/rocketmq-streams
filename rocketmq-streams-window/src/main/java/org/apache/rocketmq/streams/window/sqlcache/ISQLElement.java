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

package org.apache.rocketmq.streams.window.sqlcache;

import java.util.List;

public interface ISQLElement {

    boolean isWindowInstanceSQL();

    /**
     * window max value for max offset
     * @return
     */
    boolean isSplitSQL();

    /**
     * fire message, can cancel not commit sqls which owned the windowinstance
     * @return
     */
    boolean isFireNotify();


    String getQueueId();


    String getWindowInstanceId();


    String getSQL();


    Integer getIndex();


    void setIndex(int index);
}
