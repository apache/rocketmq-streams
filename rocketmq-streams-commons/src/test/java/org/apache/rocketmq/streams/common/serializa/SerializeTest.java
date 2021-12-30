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
package org.apache.rocketmq.streams.common.serializa;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.streams.common.datatype.ArrayDataType;
import org.apache.rocketmq.streams.common.datatype.ListDataType;
import org.apache.rocketmq.streams.common.datatype.MapDataType;
import org.apache.rocketmq.streams.common.datatype.SetDataType;
import org.apache.rocketmq.streams.common.utils.SerializeUtil;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;

public class SerializeTest {

    @Test
    public void serializePerson() {
        Person person = Person.createPerson("test");
        byte[] bytes = SerializeUtil.serialize(person);
        person = SerializeUtil.deserialize(bytes);

        assertTrue(person.name.equals("Chris") && person.age == 18 && person.getAddresses().size() == 2 && person.getChildName2Age().size() == 2 && person.accums.get("count").count == 1);
    }

    @Test
    public void serializeList() {
        List list = new ArrayList();
        list.add("name");
        list.add(1);
        ListDataType listDataType = new ListDataType();
        String str = listDataType.toDataJson(list);
        list = listDataType.getData(str);
        assertTrue(list.get(0).equals("name") && list.get(1).equals(1));

        byte[] bytes = SerializeUtil.serialize(list);
        list = SerializeUtil.deserialize(bytes);
        assertTrue(list.get(0).equals("name") && list.get(1).equals(1));

        str = listDataType.toDataStr(list);
        list = listDataType.getData(str);
        assertTrue(list.get(0).equals("name") && list.get(1).equals("1"));

    }

    @Test
    public void serializeSet() {
        Set set = new HashSet();
        set.add("name");
        set.add(1);
        SetDataType setDataType = new SetDataType();
        String str = setDataType.toDataJson(set);
        set = setDataType.getData(str);
        Iterator it = set.iterator();
        Object value1 = it.next();
        Object value2 = it.next();
        assertTrue(value1.equals("name") || value1.equals(1));
        assertTrue(value2.equals("name") || value2.equals(1));

        byte[] bytes = SerializeUtil.serialize(set);
        set = SerializeUtil.deserialize(bytes);
        it = set.iterator();
        value1 = it.next();
        value2 = it.next();
        assertTrue(value1.equals("name") || value1.equals(1));
        assertTrue(value2.equals("name") || value2.equals(1));

        SetDataType dataType = new SetDataType();
        str = dataType.toDataStr(set);
        set = dataType.getData(str);
        it = set.iterator();
        value1 = it.next();
        value2 = it.next();
        assertTrue(value1.equals("name") || value1.equals("1"));
        assertTrue(value2.equals("name") || value2.equals("1"));

    }

    @Test
    public void serializeArray() {
        Long[] x = new Long[2];
        x[0] = 1L;
        x[1] = 2l;
        byte[] bytes = SerializeUtil.serialize(x);
        x = SerializeUtil.deserialize(bytes);
        assertTrue(x[0] == 1 && x[1] == 2);

        ArrayDataType arrayDataType = new ArrayDataType();
        String str = arrayDataType.toDataJson(x);
        x = (Long[]) arrayDataType.getData(str);
        assertTrue(x[0] == 1 && x[1] == 2);

        str = arrayDataType.toDataStr(x);
        String[] xx = (String[]) arrayDataType.getData(str);

        assertTrue(xx[0].equals("1") && xx[1].equals("2"));
    }

    @Test
    public void serializeHashMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("name", "chris");
        map.put("age", 18);
        map.put("isMale", true);
        MapDataType mapDataType = new MapDataType();
        String str = mapDataType.toDataJson(map);
        map = mapDataType.getData(str);
        assertTrue(map.get("name").equals("chris") && (Integer) map.get("age") == 18 && (Boolean) map.get("isMale") == true);

        byte[] bytes = SerializeUtil.serialize(map);
        map = SerializeUtil.deserialize(bytes);
        assertTrue(map.get("name").equals("chris") && (Integer) map.get("age") == 18 && (Boolean) map.get("isMale") == true);

        str = mapDataType.toDataStr(map);
        map = mapDataType.getData(str);
        assertTrue(map.get("name").equals("chris") && map.get("age").equals("18") && map.get("isMale").equals("true"));
    }
}
