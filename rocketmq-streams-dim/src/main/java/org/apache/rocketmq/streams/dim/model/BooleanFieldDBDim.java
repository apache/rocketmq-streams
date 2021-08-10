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
package org.apache.rocketmq.streams.dim.model;

import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.metadata.MetaDataField;

/**
 * 类似情报表，除了一个核心比对字段和主键，其他都是boolean类型的/或者是只有0/1两个值的字符串和int。可以用这个存储结构
 */
public class BooleanFieldDBDim extends DBDim {

    private static final Log LOG = LogFactory.getLog(BooleanFieldDBDim.class);

    /**
     * 类似情报表，除了一个核心比对字段和主键，其他都是boolean类型的。可以用这个存储结构
     *
     * @param metaData
     * @param notBooleanFieldName
     * @return
     */
    public static boolean support(MetaData metaData, String notBooleanFieldName) {
        List<MetaDataField> metaDataFields = metaData.getMetaDataFields();
        for (MetaDataField metaDataField : metaDataFields) {
            if (metaDataField.getIsPrimary()) {
                continue;
            }
            if (metaDataField.getFieldName().equals(notBooleanFieldName)) {
                continue;
            }
            if (!metaDataField.getDataType().matchClass(Boolean.class)) {
                return false;
            }
        }
        return true;
    }
}
