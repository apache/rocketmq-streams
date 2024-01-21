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
package org.apache.rocketmq.streams.script.function.model;

public enum FunctionType {

    /**
     * 1:1
     */
    UDF("udf"),

    /**
     * 1:n
     */
    UDTF("f"),

    /**
     * n:1
     */
    UDAF("udaf"),

    /**
     * built in function
     */
    BUILTIN("builtin"),

    /**
     * mostly illegal function
     */
    UNKNOWN("unknow");
    private String name;

    private FunctionType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
