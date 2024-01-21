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

import java.util.Date;
import org.apache.rocketmq.streams.db.driver.DriverBuilder;
import org.apache.rocketmq.streams.db.driver.JDBCDriver;
import org.apache.rocketmq.streams.lease.LeaseComponent;
import org.apache.rocketmq.streams.lease.model.LeaseInfo;
import org.apache.rocketmq.streams.lease.service.ILeaseGetCallback;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class LeaseTest {

    protected String USER_NAME = "";
    protected String PASSWORD = "";
    private String URL = "";

    public LeaseTest() {

        //正式使用时，在配置文件配置
//        ComponentCreator.getProperties().put(ConfigureFileKey.JDBC_URL, URL);//数据库连接url
//        ComponentCreator.getProperties().put(ConfigureFileKey.JDBC_USERNAME, USER_NAME);//用户名
//        ComponentCreator.getProperties().put(ConfigureFileKey.JDBC_PASSWORD, PASSWORD);//password

        /**
         * 创建lease info表
         */
        JDBCDriver driver = DriverBuilder.createDriver();
        driver.execute(LeaseInfo.createTableSQL());
    }

    @Test
    public void testLease() throws InterruptedException {
        String leaseName = "lease.test";
        int leaseTime = 5;
        LeaseComponent.getInstance().getService().startLeaseTask(leaseName, leaseTime, new ILeaseGetCallback() {
            @Override
            public void callback(Date nextLeaseDate) {
                System.out.println("I get lease");
            }
        });
        assertTrue(LeaseComponent.getInstance().getService().hasLease(leaseName));
        Thread.sleep(5000);
        assertTrue(LeaseComponent.getInstance().getService().hasLease(leaseName));//会一直续约
        Thread.sleep(5000);
        assertTrue(LeaseComponent.getInstance().getService().hasLease(leaseName));//会一直续约
    }

    @Test
    public void testLock() throws InterruptedException {
        String name = "dipper";
        String lockName = "lease.test";
        int leaseTime = 5;
        boolean success = LeaseComponent.getInstance().getService().lock(name, lockName, leaseTime);//锁定5秒钟
        assertTrue(success);//获取锁
        Thread.sleep(6000);
        assertTrue(!LeaseComponent.getInstance().getService().hasHoldLock(name, lockName));//超期释放
    }

    /**
     * holdlock是一直持有锁，和租约的区别是，当释放锁后，无其他实例抢占
     *
     * @throws InterruptedException
     */
    @Test
    public void testHoldLock() throws InterruptedException {
        String name = "dipper";
        String lockName = "lease.test";
        int leaseTime = 6;
        boolean success = LeaseComponent.getInstance().getService().holdLock(name, lockName, leaseTime);//锁定5秒钟
        assertTrue(success);//获取锁
        Thread.sleep(8000);
        assertTrue(LeaseComponent.getInstance().getService().hasHoldLock(name, lockName));//会自动续约，不会释放，可以手动释放
        LeaseComponent.getInstance().getService().unlock(name, lockName);
        assertTrue(!LeaseComponent.getInstance().getService().hasHoldLock(name, lockName));
    }
}
