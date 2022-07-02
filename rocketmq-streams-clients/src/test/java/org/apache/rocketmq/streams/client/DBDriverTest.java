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

package org.apache.rocketmq.streams.client;

import org.apache.rocketmq.streams.configurable.ConfigurableComponent;
import org.junit.Test;

import static junit.framework.TestCase.assertNotNull;


public class DBDriverTest {

    @Test
    public void testDBConfigurableService() {
        String namespace = "streams.db.configurable";

        ConfigurableComponent configurableComponent = ConfigurableComponent.getInstance("2211");
        configurableComponent.insert(createPerson(namespace));
        configurableComponent.refreshConfigurable(namespace);
        Person person = configurableComponent.queryConfigurable("person", "peronName");
        System.out.println(person.getName());
        System.out.println(person.getAge());

        assertNotNull(person);
    }

    /**
     * 创建configuable对象
     *
     * @param namespace
     * @return
     */
    protected Person createPerson(String namespace) {
        Person person = new Person();
        person.setName("chris");
        person.setAge(18);
        person.setNameSpace(namespace);
        person.setConfigureName("peronName");
        person.setType("person");
        return person;
    }
}
