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
package org.apache.rocketmq.streams.script.function.function;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JavaObjectUDFFunction {

    /**
     * 这个是UDF
     * @param person 这个是数据流中的列值
     * @return
     */
    public String eval(Person person){
        return person.name;
    }

    /**
     * 一个类中，UDF可以多个
     * @param person
     * @param needAge
     * @return
     */
    public String eval(Person person,boolean needAge){
        return person.name+(needAge?person.age:"");
    }

    /**
     * 这个是UDTF，UDTF如果
     * @param person
     * @param splitSign
     * @return 返回的是拆分后的多行数据，包含多列，
     * 每列是一个<key，value>数据，如果没有key的值，请用f0，f1，f2，fn来当作默认的字段名
     */
    public  List<Map<String,Object>> eval(Person person,String splitSign){
        String name=person.name;
        String[] names=name.split(splitSign);
        List<Map<String,Object>> rows=new ArrayList<>();
        for(int i=0;i<names.length;i++){
            Map<String,Object> row=new HashMap<>();
            row.put("f"+i,names[i]);
            rows.add(row);
        }
        return rows;
    }
}
