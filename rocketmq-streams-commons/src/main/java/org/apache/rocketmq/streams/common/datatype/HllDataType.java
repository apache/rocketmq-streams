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
package org.apache.rocketmq.streams.common.datatype;

import com.alibaba.fastjson.JSONObject;

import net.agkn.hll.HLL;
import org.apache.rocketmq.streams.common.utils.Base64Utils;
import org.apache.rocketmq.streams.common.utils.StringUtil;

/**
 * @author arthur.liang
 */
public class HllDataType extends BaseDataType<HLL> {

    public HllDataType(Class clazz) {
        setDataClazz(clazz);
    }

    public HllDataType() {
        setDataClazz(HLL.class);
    }

    @Override
    protected void setFieldValueToJson(JSONObject jsonObject) {
    }

    @Override
    protected void setFieldValueFromJson(JSONObject jsonObject) {

    }

    @Override
    public DataType create() {
        return this;
    }

    @Override
    public String getDataTypeName() {
        return HLL.class.getSimpleName();
    }

    @Override
    public boolean matchClass(Class clazz) {
        return HLL.class.getSimpleName().equals(clazz.getSimpleName());
    }

    @Override
    public String toDataJson(HLL value) {
        if (value != null) {
            return Base64Utils.encode(value.toBytes());
        }
        return null;
    }

    @Override
    public HLL getData(String jsonValue) {
        if (StringUtil.isNotEmpty(jsonValue)) {
            return HLL.fromBytes(Base64Utils.decode(jsonValue));
        }
        return null;
    }

    public static void main(String[] args) {
        HLL hll = new HLL(30, 8);
        hll.addRaw(123456);
        HllDataType dataType = new HllDataType();
        String content = dataType.toDataJson(hll);
        HLL copy = dataType.getData(content);
        System.out.println(copy.cardinality());
    }

}
