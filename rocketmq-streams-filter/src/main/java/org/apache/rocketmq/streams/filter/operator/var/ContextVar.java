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
package org.apache.rocketmq.streams.filter.operator.var;

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.metadata.MetaDataField;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.filter.context.RuleContext;
import org.apache.rocketmq.streams.filter.operator.Rule;

public class ContextVar<T> extends Var<T> {

    private static final long serialVersionUID = -7025012350292140132L;
    private String fieldName;
    private String metaDataName;                            // 消息队列对应的meta信息

    @SuppressWarnings("unchecked")
    @Override
    public T doAction(RuleContext context, Rule rule) {
        if (!volidate(context, rule)) {
            return null;
        }
        JSONObject message = context.getMessage().getMessageBody();
        Object object = null;
        MetaData metaData = context.getMetaData(metaDataName);
        if (metaData == null) {
            object = message.get(fieldName);
            if (object == null) {
                object = ReflectUtil.getBeanFieldOrJsonValue(message, String.class, fieldName);
            }
            return (T)object;
        }
        Class dataClass = String.class;
        MetaDataField field = metaData.getMetaDataField(fieldName);
        if (field != null) {
            dataClass = field.getDataType().getDataClass();
        }
        object = ReflectUtil.getBeanFieldOrJsonValue(message, dataClass, fieldName);
        if (object == null) {
            return null;
        }
        return (T)object;
    }

    @Override
    public boolean volidate(RuleContext context, Rule rule) {
        return true;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getMetaDataName() {
        return metaDataName;
    }

    public void setMetaDataName(String metaDataName) {
        this.metaDataName = metaDataName;
    }

    @Override
    public boolean canLazyLoad() {
        return false;
    }
}
