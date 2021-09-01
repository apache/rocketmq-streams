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

package org.apache.rocketmq.streams.configuable;

import java.util.List;
import org.apache.rocketmq.streams.common.component.AbstractComponent;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.configuable.model.Person;
import org.apache.rocketmq.streams.configurable.ConfigurableComponent;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;

public class ConfiguableComponentTest {

    @Test
    public void testInsertConfiguable(){
        String namespace="org.apache.configuable.test";
        ConfigurableComponent configurableComponent= ConfigurableComponent.getInstance(namespace);
        Person person=createPerson(namespace);
        configurableComponent.insert(person);//完成数据存储，在配置文件配置存储类型，支持内存，db和文件，默认是内存
        //查询只操作内存，存储的数据定时加载到内存，刚插入的数据，还未加载，查询不到
        assertTrue(configurableComponent.queryConfigurable("person","personName")==null);
        configurableComponent.refreshConfigurable(namespace);//强制加载数据到内存，可以查询数据
        assertTrue(configurableComponent.queryConfigurable("person","peronName")!=null);
    }


    @Test
    public void testConfiguableENVDependence(){
        String namespace="org.apache.configuable.test";
        ConfigurableComponent configurableComponent= ConfigurableComponent.getInstance(namespace);
        Person person=createPerson(namespace);
        person.setName("persion.name");//对于有ENVDependence的字段，可以不存储真值，存储一个key，把真值配置在配置文件中
        configurableComponent.insert(person);//完成数据存储，在配置文件配置存储类型，支持内存，db和文件，默认是内存
        ComponentCreator.getProperties().put("persion.name","realName");//这个代表真实的配置文件，启动时会把配置文件的内容加载到ComponentCreator.getProperties()中
        configurableComponent.refreshConfigurable(namespace);//刷新存储
        person=configurableComponent.queryConfigurable("person","peronName");
        assertTrue(person.getName().equals("realName"));
    }


    @Test
    public void testSupportParentNameSpace(){
        String namespace="org.apache.configuable.test";
        ConfigurableComponent configurableComponent= ConfigurableComponent.getInstance(namespace);
        Person person=createPerson(namespace);
        Person otherPerson=createPerson("org.apache.configuable.test1");
        configurableComponent.insert(person);
        configurableComponent.insert(otherPerson);
        configurableComponent.refreshConfigurable(namespace);
        //只加载自己命名空间的对象
        List<Person> personList=configurableComponent.queryConfigurableByType("person");
        assertTrue(personList.size()==1);

        /**
         * 顶级命名空间的对象，所有namespace都可见
         */
        Person thirdPerson=createPerson(IConfigurableService.PARENT_CHANNEL_NAME_SPACE);
        configurableComponent.insert(thirdPerson);
        configurableComponent.refreshConfigurable(namespace);//只加载自己命名空间的对象
        personList=configurableComponent.queryConfigurableByType("person");
        assertTrue(personList.size()==2);
    }


    //测试定时加载逻辑，当对象的updateFlag值变化后，才会被替换旧对象
    @Test
    public void testAutoLoader() throws InterruptedException {
        ComponentCreator.getProperties().put(AbstractComponent.POLLING_TIME,"1");//1秒后动态加载对象
        String namespace="org.apache.configuable.test";
        ConfigurableComponent configurableComponent= ConfigurableComponent.getInstance(namespace);
        Person person=createPerson(namespace);
        configurableComponent.insert(person);
        Thread.sleep(2000);//1秒后，新插入的对象会被加载
        person=configurableComponent.queryConfigurable("person","peronName");
        assertTrue(person!=null);


    }

    /**
     * 创建configuable对象
     * @param namespace
     * @return
     */
    protected Person createPerson(String namespace){
        Person person=new Person();
        person.setName("chris");
        person.setAge(18);
        person.setNameSpace(namespace);
        person.setConfigureName("peronName");
        person.setType("person");
        return person;
    }
}
