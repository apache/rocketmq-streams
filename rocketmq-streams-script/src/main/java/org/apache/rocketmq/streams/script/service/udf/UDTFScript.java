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
package org.apache.rocketmq.streams.script.service.udf;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.utils.DataTypeUtil;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.function.model.FunctionType;
import org.apache.rocketmq.streams.script.function.service.IFunctionService;

/**
 * 核心思路：通过实现外部流计算的uatf的收集器，把拆分的流对象收集到自己的上下文实现
 */
public abstract class UDTFScript extends UDFScript {

    /**
     * 一般的外部udtf都有一个收集器，负责把拆分的数据传输出去，这个是收集器的set 方法名
     */
    protected String setCollectorMethodName;
    /**
     * 格式 可为空 type:value,type:value
     */
    protected String resultTypeInitParamters;
    /**
     * 解析上方的字符串生成结果
     */
    protected transient List resultTypes = new ArrayList();
    /**
     * 解析上方的字符串生成结果
     */
    protected transient List<Class> resultTypeClasses = new ArrayList<>();
    /**
     * 解析上方的字符串生成结果
     */
    protected transient DataType[] resultTypeDataTypes;
    /**
     * 是否完成初始化
     */
    private transient volatile boolean hasInit = false;

    @Override
    protected boolean initBeanClass(IFunctionService iFunctionService) {
        super.initBeanClass(iFunctionService);
        ReflectUtil.invoke(instance, setCollectorMethodName, new Class[] {getCollectorClasss()},
            new Object[] {getCollector()});
        if (StringUtil.isNotEmpty(resultTypeInitParamters)) {
            String[] values = resultTypeInitParamters.split(",");
            resultTypeDataTypes = new DataType[values.length];
            for (int i = 0; i < values.length; i++) {
                String[] kv = values[i].split(":");
                if (kv.length == 2) {
                    String typeName = kv[0];
                    DataType dataType = DataTypeUtil.getDataType(typeName);
                    resultTypeDataTypes[i] = dataType;
                    resultTypes.add(dataType.getData(kv[1]));
                    resultTypeClasses.add(dataType.getDataClass());
                }
            }
        }
        return true;
    }

    @Override
    protected FunctionType getFunctionType() {
        return FunctionType.UDTF;
    }

    protected abstract Class getCollectorClasss();

    protected abstract Object getCollector();

    public String getSetCollectorMethodName() {
        return setCollectorMethodName;
    }

    public void setSetCollectorMethodName(String setCollectorMethodName) {
        this.setCollectorMethodName = setCollectorMethodName;
    }

    public String getResultTypeInitParamters() {
        return resultTypeInitParamters;
    }

    public void setResultTypeInitParamters(String resultTypeInitParamters) {
        this.resultTypeInitParamters = resultTypeInitParamters;
    }

    public List<Object> getResultTypeObject() {
        return resultTypes;
    }

    public List<Class> getResultTypeClass() {
        return resultTypeClasses;
    }
}
