package org.apache.rocketmq.streams.configruation.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;

/**
 * Created by yuanxiaodong on 6/27/19.
 */
public class Person extends BasedConfigurable {
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
}
