package org.apache.rocketmq.streams.common.monitor.model;

import com.alibaba.fastjson.annotation.JSONField;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Description:
 * @author 苏同亮
 * Date 2021-06-21
 */
public class TraceMonitorDO {

    /**
     * 主键
     */
    private int id;

    /**
     * traceid
     */
    private String traceId;

    /**
     * stagename
     */
    private String stageName;

    /**
     * 输入数量
     */
    private int inputNumber;
    private AtomicInteger safeInput = new AtomicInteger();
    /**
     * 输入最后一条消息
     */
    private String inputLastMsg;

    /**
     * 输出数量
     */
    private int outputNumber;
    private AtomicInteger safeOutput = new AtomicInteger();

    /**
     * 输出最后一条消息
     */
    private String outputLastMsg;

    /**
     * 最后一条输入时间
     */
    @JSONField(format="yyyy-MM-dd HH:mm:ss")
    private Date lastInputMsgTime;

    /**
     * 最后一条输出时间
     */
    @JSONField(format="yyyy-MM-dd HH:mm:ss")
    private Date lastOutputMsgTime;

    /**
     * 当前 stage 的消息流转状态
     * -1 消息在此 stage 被过滤掉
     *  0 消息为流转到此 stage
     *  1 正常流转
     */
    private Integer status;

    /**
     * 异常信息
     */
    private String exceptionMsg;

    /**
     * 此 traceId 对应的 jobName
     */
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
     * setter for column traceid
     */
    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    /**
     * getter for column traceid
     */
    public String getTraceId() {
        return this.traceId;
    }

    /**
     * setter for column stagename
     */
    public void setStageName(String stageName) {
        this.stageName = stageName;
    }

    /**
     * getter for column stagename
     */
    public String getStageName() {
        return this.stageName;
    }

    /**
     * setter for column 输入数量
     */
    public void setInputNumber(int inputNumber) {
        this.inputNumber = inputNumber;
    }

    /**
     * getter for column 输入数量
     */
    public int getInputNumber() {
        return this.inputNumber;
    }

    /**
     * setter for column 输入最后一条消息
     */
    public void setInputLastMsg(String inputLastMsg) {
        this.inputLastMsg = inputLastMsg;
    }

    /**
     * getter for column 输入最后一条消息
     */
    public String getInputLastMsg() {
        return this.inputLastMsg;
    }

    /**
     * setter for column 输出数量
     */
    public void setOutputNumber(int outputNumber) {
        this.outputNumber = outputNumber;
    }

    /**
     * getter for column 输出数量
     */
    public int getOutputNumber() {
        return this.outputNumber;
    }

    /**
     * setter for column 输出最后一条消息
     */
    public void setOutputLastMsg(String outputLastMsg) {
        this.outputLastMsg = outputLastMsg;
    }

    /**
     * getter for column 输出最后一条消息
     */
    public String getOutputLastMsg() {
        return this.outputLastMsg;
    }

    public Date getLastInputMsgTime() {
        return lastInputMsgTime;
    }

    public void setLastInputMsgTime(Date lastInputMsgTime) {
        this.lastInputMsgTime = lastInputMsgTime;
    }

    public Date getLastOutputMsgTime() {
        return lastOutputMsgTime;
    }

    public void setLastOutputMsgTime(Date lastOutputMsgTime) {
        this.lastOutputMsgTime = lastOutputMsgTime;
    }

    public AtomicInteger getSafeInput() {
        return safeInput;
    }

    public void setSafeInput(AtomicInteger safeInput) {
        this.safeInput = safeInput;
    }

    public AtomicInteger getSafeOutput() {
        return safeOutput;
    }

    public void setSafeOutput(AtomicInteger safeOutput) {
        this.safeOutput = safeOutput;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getExceptionMsg() {
        return exceptionMsg;
    }

    public void setExceptionMsg(String exceptionMsg) {
        this.exceptionMsg = exceptionMsg;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }
}
