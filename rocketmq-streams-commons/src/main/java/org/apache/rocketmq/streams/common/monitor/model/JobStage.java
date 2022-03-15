package org.apache.rocketmq.streams.common.monitor.model;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.annotation.JSONField;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Description:
 * @author 苏同亮
 * Date 2021-05-14
 */
public class JobStage {

    /**
     * 主键
     */
    private int id;

    /**
     * 任务名称
     */
    private String jobName;

    /**
     * 机器名称
     */
    private String machineName;

    /**
     * stage名称
     */
    private String stageName;

    /**
     * stage类型
     */
    private String stageType;

    /**
     * 输入数据量
     */
    private long input;
    private long prevInput;
    private AtomicInteger safeInput = new AtomicInteger();

    /**
     * 输出数据量
     */
    private long output;
    private AtomicInteger safeOutput = new AtomicInteger();

    /**
     * 消息处理速度
     */
    private double tps;

    /**
     * 最后输入数据
     */
    private String lastInputMsg;
    private JSONObject lastInputMsgObj;

    /**
     * 最后输入数据时间
     */
    @JSONField(format="yyyy-MM-dd HH:mm:ss")
    private Date lastInputMsgTime;

    /**
     * 最后输出数据时间
     */
    @JSONField(format="yyyy-MM-dd HH:mm:ss")
    private Date lastOutputMsgTime;

    /**
     * stage下游集合
     */
    private String nextStageLables;

    /**
     * stage上游集合
     */
    private String prevStageLables;

    /**
     * stage内容
     */
    private String stageContent;

    @JSONField(format="yyyy-MM-dd HH:mm:ss")
    private Date pingTime;

    private long createTime = System.currentTimeMillis();

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
     * setter for column 任务名称
     */
    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    /**
     * getter for column 任务名称
     */
    public String getJobName() {
        return this.jobName;
    }

    /**
     * setter for column 机器名称
     */
    public void setMachineName(String machineName) {
        this.machineName = machineName;
    }

    /**
     * getter for column 机器名称
     */
    public String getMachineName() {
        return this.machineName;
    }

    /**
     * setter for column stage名称
     */
    public void setStageName(String stageName) {
        this.stageName = stageName;
    }

    /**
     * getter for column stage名称
     */
    public String getStageName() {
        return this.stageName;
    }

    /**
     * setter for column stage类型
     */
    public void setStageType(String stageType) {
        this.stageType = stageType;
    }

    /**
     * getter for column stage类型
     */
    public String getStageType() {
        return this.stageType;
    }

    /**
     * setter for column 输入数据量
     */
    public void setInput(long input) {
        this.input = input;
    }

    /**
     * getter for column 输入数据量
     */
    public long getInput() {
        return this.input;
    }

    /**
     * setter for column 输出数据量
     */
    public void setOutput(long output) {
        this.output = output;
    }

    /**
     * getter for column 输出数据量
     */
    public long getOutput() {
        return this.output;
    }

    public double getTps() {
        return tps;
    }

    public void setTps(double tps) {
        this.tps = tps;
    }

    /**
     * setter for column 最后输入数据
     */
    public void setLastInputMsg(String lastInputMsg) {
        this.lastInputMsg = lastInputMsg;
    }

    /**
     * getter for column 最后输入数据
     */
    public String getLastInputMsg() {
        return this.lastInputMsg;
    }

    /**
     * setter for column 最后输入数据时间
     */
    public void setLastInputMsgTime(Date lastInputMsgTime) {
        this.lastInputMsgTime = lastInputMsgTime;
    }

    /**
     * getter for column 最后输入数据时间
     */
    public Date getLastInputMsgTime() {
        return this.lastInputMsgTime;
    }

    /**
     * setter for column 最后输出数据时间
     */
    public void setLastOutputMsgTime(Date lastOutputMsgTime) {
        this.lastOutputMsgTime = lastOutputMsgTime;
    }

    /**
     * getter for column 最后输出数据时间
     */
    public Date getLastOutputMsgTime() {
        return this.lastOutputMsgTime;
    }

    /**
     * setter for column stage下游集合
     */
    public void setNextStageLables(String nextStageLables) {
        this.nextStageLables = nextStageLables;
    }

    /**
     * getter for column stage下游集合
     */
    public String getNextStageLables() {
        return this.nextStageLables;
    }

    /**
     * setter for column stage上游集合
     */
    public void setPrevStageLables(String prevStageLables) {
        this.prevStageLables = prevStageLables;
    }

    /**
     * getter for column stage上游集合
     */
    public String getPrevStageLables() {
        return this.prevStageLables;
    }

    /**
     * setter for column stage内容
     */
    public void setStageContent(String stageContent) {
        this.stageContent = stageContent;
    }

    /**
     * getter for column stage内容
     */
    public String getStageContent() {
        return this.stageContent;
    }

    public Date getPingTime() {
        return pingTime;
    }

    public void setPingTime(Date pingTime) {
        this.pingTime = pingTime;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public JSONObject getLastInputMsgObj() {
        return lastInputMsgObj;
    }

    public void setLastInputMsgObj(JSONObject lastInputMsgObj) {
        this.lastInputMsgObj = lastInputMsgObj;
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

    public long getPrevInput() {
        return prevInput;
    }

    public void setPrevInput(long prevInput) {
        this.prevInput = prevInput;
    }
}