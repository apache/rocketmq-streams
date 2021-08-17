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
package org.apache.rocketmq.streams.configurable.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
<<<<<<< HEAD

=======
>>>>>>> e0ae8a24f70a6cd27b9c35f1709fb7b3fbe42269
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;

public class Person extends BasedConfigurable {
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
        person.setConfigureName("Chris");
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
        return "Person{" + "name='" + name + '\'' + ", age=" + age + ", isMale=" + isMale + ", addresses=" + addresses
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
            person = (Person)super.clone();
        } catch (CloneNotSupportedException e) {
            System.out.println("clone error " + e);
        }
        return person;
    }
}
