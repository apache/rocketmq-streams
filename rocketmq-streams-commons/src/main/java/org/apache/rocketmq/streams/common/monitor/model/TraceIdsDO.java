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
package org.apache.rocketmq.streams.common.monitor.model;

import com.alibaba.fastjson.annotation.JSONField;
import java.util.Date;

/**
 * Description:
 *
 * @author 苏同亮
 * Date 2021-06-21
 */
public class TraceIdsDO {

    /**
     * 主键
     */
    private int id;

    /**
     * tarceid
     */
    private String traceId;

    /**
     * 过期时间
     */
    @JSONField(format = "yyyy-MM-dd HH:mm:ss")
    private Date gmtExpire;

    /**
     * 创建时间
     */
    @JSONField(format = "yyyy-MM-dd HH:mm:ss")
    private Date gmtCreate;

    private String useStatus;
    private String jobName;

    /**
     * setter for column 主键
     */
    public void setId(int id) {
        this.id = id;
    }

    /**
     * getter for column 主键
     */
    public int getId() {
        return this.id;
    }

    /**
     * setter for column tarceid
     */
    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    /**
     * getter for column tarceid
     */
    public String getTraceId() {
        return this.traceId;
    }

    /**
     * setter for column 过期时间
     */
    public void setGmtExpire(Date gmtExpire) {
        this.gmtExpire = gmtExpire;
    }

    /**
     * getter for column 过期时间
     */
    public Date getGmtExpire() {
        return this.gmtExpire;
    }

    /**
     * setter for column 创建时间
     */
    public void setGmtCreate(Date gmtCreate) {
        this.gmtCreate = gmtCreate;
    }

    /**
     * getter for column 创建时间
     */
    public Date getGmtCreate() {
        return this.gmtCreate;
    }

    public String getUseStatus() {
        return useStatus;
    }

    public void setUseStatus(String useStatus) {
        this.useStatus = useStatus;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }
}
