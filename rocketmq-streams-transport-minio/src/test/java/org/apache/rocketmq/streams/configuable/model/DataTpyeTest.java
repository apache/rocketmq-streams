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

package org.apache.rocketmq.streams.configuable.model;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.junit.Test;

public class DataTpyeTest {
    @Test
    public void testDataType() {
        Person person = Person.createPerson("com.dipper.test");
        String jsonValue = person.toJson();

        Person person1 = new Person();
        person1.toObject(jsonValue);
        System.out.println(person1);
    }

    @Test
    public void testV2() {
        Set<String> set = new HashSet<>();
        set.add("北斗");
        set.add("福建jz");
        set.add("甘肃jz");
        set.add("广东省气象micaps云");
        set.add("贵州公安科信");
        set.add("贵州警务云");
        set.add("杭州税友");
        set.add("江西公安大数据平台");
        set.add("昆仑项目");
        set.add("新华网");
        set.add("浙江气象高时空分辨率气象预报专有云");
        List<String> v2 = FileUtil.loadFileLine("/Users/yuanxiaodong/Documents/workdir/项目名称.txt");
        List<String> zyy = FileUtil.loadFileLine("/Users/yuanxiaodong/Documents/workdir/专有云.txt");
        int count = 0;
        for (String v2Line : v2) {
            boolean match = false;
            for (String zyyLine : zyy) {
                if (zyyLine.indexOf(v2Line) != -1 || v2Line.indexOf(zyyLine) != -1) {
                    match = true;
                    count++;
                }
            }
            if (match == false) {
                System.out.println(v2Line);
            }
        }
        System.out.println(count);
    }
}
