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
package org.apache.rocketmq.streams.common.dboperator;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * 提供操作数据库的常用方法。
 */
public interface IDBDriver {

    /**
     * 执行数据库更新，返回更新的条数
     *
     * @param sql sql
     * @return 更新条数
     */
    int update(String sql);

    /**
     * 执行sql，可以是任何非查询sql，无返回值
     *
     * @param sql sql
     */
    void execute(String sql);

    /**
     * 以预编译的方式执行sql
     *
     * @param sql    含参数的sql
     * @param params 参数
     */
    int execute(String sql, Object[] params);

    /**
     * 以预编译的方式执行查询
     *
     * @param sql    含参数的sql
     * @param params 参数
     */
    List<Map<String, Object>> executeQuery(String sql, Object[] params);

    /**
     * 查询多行数据
     *
     * @param sql sql
     * @return 多行数据，一行数据用Map<String,Object> 表示
     */
    List<Map<String, Object>> queryForList(String sql);

    /**
     * 返回单行数据，如果查询结果是多行会抛出错误。
     *
     * @param sql sql
     * @return 单行数据
     */
    Map<String, Object> queryOneRow(String sql);

    /**
     * 执行insert，replace语句。
     *
     * @param sql sql
     * @return 必要时返回插入的id
     */
    long executeInsert(final String sql);

    void executeSqls(String... sqls);

    void executeSqls(Collection<String> sqls);

    /**
     * 分批查询数据，每次批次是batchSize，避免一次加载数据太多
     *
     * @param sql       sql
     * @param batchSize 每批次加载的数据量
     * @return 加载的全部数据
     */
    List<Map<String, Object>> batchQueryBySql(String sql, int batchSize);
}
