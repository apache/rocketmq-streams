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
package org.apache.rocketmq.streams.common.interfaces;

public interface IJDBCOperator {
    String DATA = "_data";//update返回值中的key
    String SUCCESS = "_success_status";//执行是否出错，值对应的是boolean
    String ERROR_MESSAGE = "_error_message";//如果出错了，出错的详细信息
    String DATA_TYPE = "_data_type";//返回的类型

    /**
     * 完成sql的更新操作
     *
     * @param url
     * @param userName
     * @param password
     * @param sql
     * @return jsonobject<key:UPDATE_COUNT_KEY, value:int>
     */
    String update(String url, String userName, String password, String sql);

    /**
     * 完成sql的执行
     *
     * @param url
     * @param userName
     * @param password
     * @param sql
     */
    String execute(String url, String userName, String password, String sql);

    /**
     * 完成sql插入
     *
     * @param url
     * @param userName
     * @param password
     * @param sql
     * @return
     */
    String excuteInsert(String url, String userName, String password, final String sql);

    /**
     * 查询数据
     *
     * @param url
     * @param userName
     * @param password
     * @param sql
     * @return jsonArray对象
     */
    String queryForList(String url, String userName, String password, String sql);

    /**
     * 执行sql，按datatype做值的返回
     *
     * @param sql
     * @param dataTypeStr
     * @return
     */
    String queryForObject(String url, String userName, String password, String sql, String dataTypeStr);

}
