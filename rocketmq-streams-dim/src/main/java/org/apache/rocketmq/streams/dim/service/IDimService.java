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
package org.apache.rocketmq.streams.dim.service;

import java.util.List;
import java.util.Map;

public interface IDimService {

    /**
     * 做维表join，关系通过表达式表示，返回所有匹配的行。因为msg没有key，表达式中，以下标表示key，如0，1，2。
     *
     * @param dimName       维表的名称
     * @param expressionStr 表达式（0,functionName,filedName)&（1,functionName,filedName)|（2,functionName,filedName)
     * @param msgs          流数据
     * @return 符合匹配条件的所有行
     */
    Map<String, Object> match(String dimName, String expressionStr, Object... msgs);

    /**
     * 做维表join，关系通过表达式表示，返回所有匹配的行。
     *
     * @param dimName       维表的名称
     * @param expressionStr 表达式,varName是msg中的key名称（varName,functionName,filedName)&（varName,functionName,filedName)|（varName,functionName,filedName)
     * @param msgs          流数据
     * @return 符合匹配条件的所有行
     */
    List<Map<String, Object>> matchSupportMultiRow(String dimName, String expressionStr, Map<String, Object> msgs);

    /**
     * 做维表join，关系通过表达式表示，返回匹配的一行数据，如果有多行匹配，只返回第一行。
     *
     * @param dimName       维表的名称
     * @param expressionStr 表达式,varName是msg中的key名称（varName,functionName,filedName)&（varName,functionName,filedName)|（varName,functionName,filedName)
     * @param msgs          流数据
     * @return 返回匹配的一行数据，如果有多行匹配，只返回第一行。
     */
    Map<String, Object> match(String dimName, String expressionStr, Map<String, Object> msgs);

    /**
     * 做维表join，关系通过表达式表示，返回匹配的全部数据。
     *
     * @param dimName       维表的名称
     * @param expressionStr 表达式,varName是msg中的key名称（varName,functionName,filedName)&（varName,functionName,filedName)|（varName,functionName,filedName)
     * @param msgs          流数据
     * @param script        对维表字段做处理的函数,在执行表达式前需要先对维表字段做处理，如fiedlName=trim(fieldName)
     * @return 返回匹配的一行数据，如果有多行匹配，只返回第一行。
     */
    List<Map<String, Object>> matchSupportMultiRow(String dimName,
                                                   String expressionStr, Map<String, Object> msgs, String script);
}
