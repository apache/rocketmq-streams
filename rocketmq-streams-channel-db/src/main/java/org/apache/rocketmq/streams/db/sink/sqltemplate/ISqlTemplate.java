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
package org.apache.rocketmq.streams.db.sink.sqltemplate;

import java.util.List;
import java.util.Map;

/**
 * @description
 */
public interface ISqlTemplate {

    public static final String SQL_MODE_DEFAULT = "default";
    public static final String SQL_MODE_DUPLICATE = "duplicate";
    public static final String SQL_MODE_IGNORE = "ignore";

    static final String[] SUPPORTS = new String[] {
        ISqlTemplate.SQL_MODE_DEFAULT,
        SQL_MODE_DUPLICATE,
        SQL_MODE_IGNORE
    };

    /**
     * create sql prefix
     * eg :
     * insert into table(`a`,`b`,`c`)
     * or insert ignore into table(`a`, `b`, `c`)
     * or insert into table(`a`,`b`,`c`)    on duplicate key update
     * `a` = values(`a`), `b` = values(`b`), `c` = values(`c`)
     *
     * @return
     */
    void initSqlTemplate();

    String createSql(List<? extends Map<String, Object>> rows);

}
