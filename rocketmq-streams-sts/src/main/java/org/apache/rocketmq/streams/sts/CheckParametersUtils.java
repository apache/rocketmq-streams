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
package org.apache.rocketmq.streams.sts;

public class CheckParametersUtils<T> {

    public static final <T> T checkIfNullReturnDefault(T t1, T t2) {
        if (t1 == null) {
            return t2;
        } else {
            return t1;
        }
    }

    public static final <T> T checkIfNullThrowException(T t, String ExceptionMsg) {

        if (t == null) {
            throw new NullPointerException("check parameter error : {}. ");
        }
        return t;
    }

    public static final int checkIfNullReturnDefaultInt(Object o, int t) {

        if (o == null) {
            return t;
        }
        return Integer.parseInt(String.valueOf(o));

    }

}
