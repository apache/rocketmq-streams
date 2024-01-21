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
package org.apache.rocketmq.streams.storage.utils;

import org.apache.rocketmq.streams.common.datatype.BooleanDataType;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.datatype.DoubleDataType;
import org.apache.rocketmq.streams.common.datatype.FloatDataType;
import org.apache.rocketmq.streams.common.datatype.IntDataType;
import org.apache.rocketmq.streams.common.datatype.LongDataType;
import org.apache.rocketmq.streams.common.datatype.ShortDataType;
import org.apache.rocketmq.streams.common.datatype.StringDataType;
import org.apache.rocketmq.streams.common.utils.DataTypeUtil;

public class TermMatchUtil {

    /**
     * @param object
     * @param dataType
     * @param term
     * @return
     */
    public static boolean matchTerm(Object object, DataType dataType, Object term, boolean isSupportPrefix) {
        if (object == null || dataType == null || term == null) {
            return false;
        }
        if (StringDataType.class.isInstance(dataType)) {
            String value = object.toString();
            String termStr = term.toString();
            if (isSupportPrefix) {
                if (value.startsWith(termStr)) {
                    return true;
                } else {
                    return value.equals(termStr);
                }
            }
        } else if (LongDataType.class.isInstance(dataType) || IntDataType.class.isInstance(dataType) || ShortDataType.class.isInstance(dataType)) {
            Long value = Long.valueOf(object.toString());
            Long termLong = Long.valueOf(term.toString());
            return (value.equals(termLong));
        } else if (DoubleDataType.class.isInstance(dataType) || FloatDataType.class.isInstance(dataType)) {
            Double value = Double.valueOf(object.toString());
            Double termLong = Double.valueOf(term.toString());
            return (value.equals(termLong));
        } else if (BooleanDataType.class.isInstance(dataType)) {
            Boolean value = Boolean.valueOf(object.toString());
            Boolean termBoolean = Boolean.valueOf(term.toString());
            return value.equals(termBoolean);
        } else {
            throw new RuntimeException("can not support the datatype in term match " + dataType);
        }
        //todo other datatype
        return true;
    }

    public static boolean matchBetween(Object value, DataType type, Object min, Object max) {
        if (value == null) {
            return false;
        }
        if (DataTypeUtil.isString(type)) {
            String valueStr = value.toString();
            if (min != null) {
                String minStr = min.toString();
                if (valueStr.compareTo(minStr) < 0) {
                    return false;
                }
            }
            if (max != null) {
                String maxStr = max.toString();
                if (valueStr.compareTo(maxStr) > 0) {
                    return false;
                }
            }

        } else if (DataTypeUtil.isNumber(type)) {
            Double valueDouble = Double.valueOf(value.toString());
            if (min != null) {
                Double minDouble = Double.valueOf(min.toString());
                if (valueDouble < minDouble) {
                    return false;
                }
            }
            if (max != null) {
                Double maxDouble = Double.valueOf(max.toString());
                if (valueDouble > maxDouble) {
                    return false;
                }
            }
        } else if (DataTypeUtil.isDate(type)) {
            throw new RuntimeException("can not support date in termmath");
        } else {
            throw new RuntimeException("can not support date in termmath " + type.getDataTypeName());
        }
        return true;
    }

    public static void main(String[] args) {
        System.out.println("c".compareTo("b"));
    }
}
