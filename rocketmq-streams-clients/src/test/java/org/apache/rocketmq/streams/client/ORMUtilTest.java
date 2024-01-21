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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;
import org.apache.rocketmq.streams.db.driver.orm.ORMUtil;
import org.junit.Test;

public class ORMUtilTest {
    protected String USER_NAME = "";
    protected String PASSWORD = "";
    private String URL = "";

    public ORMUtilTest() {
        //正式使用时，在配置文件配置
//        ComponentCreator.getProperties().put(ConfigureFileKey.JDBC_URL, URL);//数据库连接url
//        ComponentCreator.getProperties().put(ConfigureFileKey.JDBC_USERNAME, USER_NAME);//用户名
//        ComponentCreator.getProperties().put(ConfigureFileKey.JDBC_PASSWORD, PASSWORD);//password
    }

    @Test
    public void testInsert() {
        String namespace = "org.apache.configuable.test";
        List<Person> personList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            personList.add(createPerson(namespace, "chris" + i));
        }
        /**
         * 不带数据库连接信息（url,userName,Password），使用ConfiguableComponet的连接信息
         */
        ORMUtil.batchIgnoreInto(personList);//批量插入，如果有唯一键冲突，替换
        ORMUtil.batchIgnoreInto(personList);//批量插入，如果有唯一键冲突，忽略
        ORMUtil.batchInsertInto(personList);////批量插入，如果有唯一键冲突，跑错
    }

    @Test
    public void testQueryList() {
        Map<String, Integer> paras = new HashMap<>();
        paras.put("age", 18);
        List<Person> personList = ORMUtil.queryForList("select * from person where age >${age} limit 100", paras, Person.class);
    }

    @Test
    public void testQueryOneRow() {
        Person personPara = new Person();
        personPara.setAge(18);
        personPara.setName("chris1");
        Person person = ORMUtil.queryForObject("select * from person where age =${age} and name='${name}' ", personPara, Person.class, URL, USER_NAME, PASSWORD);
    }

    /**
     * 创建configuable对象
     *
     * @param namespace
     * @return
     */
    protected Person createPerson(String namespace, String name) {
        Person person = new Person();
        person.setName(name);
        person.setAge(18);
        person.setNameSpace(namespace);
        person.setName("peronName");
        person.setType("person");
        return person;
    }
}

class Person extends BasedConfigurable {
    @ENVDependence
    private String name;
    private int age;
    private Boolean isMale;
    private List<String> addresses;
    private Map<String, Integer> childName2Age;

    public static Person createPerson(String namespace) {
        Person person = new Person();
        person.setNameSpace(namespace);
        person.setType("person");
        person.setName("Chris");
        person.setName("Chris");
        List<String> addresses = new ArrayList<>();
        addresses.add("huilongguan");
        addresses.add("shangdi");
        person.setAddresses(addresses);
        Map<String, Integer> childName2Age = new HashMap<>();
        childName2Age.put("yuanyahan", 8);
        childName2Age.put("yuanruxi", 4);
        person.setChildName2Age(childName2Age);
        person.setMale(true);
        person.setAge(18);
        return person;
    }

    @Override
    public String toString() {
        return "org.apache.rocketmq.streams.Person{" + "name='" + name + '\'' + ", age=" + age + ", isMale=" + isMale + ", addresses=" + addresses
            + ", childName2Age=" + childName2Age + '}';
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public Boolean getMale() {
        return isMale;
    }

    public void setMale(Boolean male) {
        isMale = male;
    }

    public List<String> getAddresses() {
        return addresses;
    }

    public void setAddresses(List<String> addresses) {
        this.addresses = addresses;
    }

    public Map<String, Integer> getChildName2Age() {
        return childName2Age;
    }

    public void setChildName2Age(Map<String, Integer> childName2Age) {
        this.childName2Age = childName2Age;
    }

    @Override
    public Object clone() {
        Person person = null;
        try {
            person = (Person) super.clone();
        } catch (CloneNotSupportedException e) {
            System.out.println("clone error " + e);
        }
        return person;
    }
}
