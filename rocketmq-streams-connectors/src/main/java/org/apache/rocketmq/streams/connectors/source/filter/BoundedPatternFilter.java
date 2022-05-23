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
package org.apache.rocketmq.streams.connectors.source.filter;

import java.io.Serializable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.connectors.model.ReaderStatus;

/**
 * @description 过滤掉已经完成的reader
 */
@Deprecated
public class BoundedPatternFilter extends AbstractPatternFilter implements Serializable {

    static final Log logger = LogFactory.getLog(BoundedPatternFilter.class);

    @Override
    public boolean filter(String sourceName, String logicTableName, String tableName) {

        ReaderStatus readerStatus = ReaderStatus.queryReaderStatusByUK(sourceName, logicTableName + "_" + tableName);
        if (readerStatus != null) {
            logger.info(String.format("filter sourceName %s, logicTableName %s, suffix %s. ", sourceName, logicTableName, tableName));
            logger.info(String.format("query result %s", readerStatus.toString()));
            return true;
        }
        if (next == null) {
            return false;
        }
        return next.filter(sourceName, logicTableName, tableName);
    }

    @Override
    public PatternFilter setNext(PatternFilter filter) {
        super.setNext(filter);
        return this;
    }

}
