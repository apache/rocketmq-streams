package org.apache.rocketmq.streams.common.monitor.model;

import com.alibaba.fastjson.annotation.JSONField;
import java.util.Date;

/**
 * Description:
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
    @JSONField(format="yyyy-MM-dd HH:mm:ss")
    private Date gmtExpire;

    /**
     * 创建时间
     */
    @JSONField(format="yyyy-MM-dd HH:mm:ss")
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
