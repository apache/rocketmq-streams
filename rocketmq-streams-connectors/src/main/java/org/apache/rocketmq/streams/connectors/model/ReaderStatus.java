package org.apache.rocketmq.streams.connectors.model;

import org.apache.rocketmq.streams.common.model.Entity;
import org.apache.rocketmq.streams.db.driver.orm.ORMUtil;

import java.util.Date;
import java.util.List;

/**
 * @description
 */
public class ReaderStatus extends Entity {

    /**
     * 查询单个readerStatus
     */
    static final String queryReaderStatusByUK = "select * from reader_status where source_name = '%s' and reader_name = '%s' and is_finished = 1";

    static final String queryReaderStatusList = "select * from reader_status where source_name = '%s' and is_finished = 1";

    static final String clearReaderStatus = "update reader_status set gmt_modified = now(), is_finished = -1 where source_name = '%s' and reader_name = '%s'";

    String sourceName;

    String readerName;

    int isFinished;

    int totalReader;

    public String getReaderName() {
        return readerName;
    }

    public void setReaderName(String readerName) {
        this.readerName = readerName;
    }

    public int getIsFinished() {
        return isFinished;
    }

    public void setIsFinished(int isFinished) {
        this.isFinished = isFinished;
    }

    public int getTotalReader() {
        return totalReader;
    }

    public void setTotalReader(int totalReader) {
        this.totalReader = totalReader;
    }

    public String getSourceName() {
        return sourceName;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    @Override
    public String toString() {
        return "ReaderStatus{" +
            "id=" + id +
            ", gmtCreate=" + gmtCreate +
            ", gmtModified=" + gmtModified +
            ", sourceName='" + sourceName + '\'' +
            ", readerName='" + readerName + '\'' +
            ", isFinished=" + isFinished +
            ", totalReader=" + totalReader +
            '}';
    }

    public static ReaderStatus queryReaderStatusByUK(String sourceName, String readerName){
        String sql = String.format(queryReaderStatusByUK, sourceName, readerName);
        ReaderStatus readerStatus = ORMUtil.queryForObject(sql, null, ReaderStatus.class);
        return readerStatus;
    }

    public static List<ReaderStatus> queryReaderStatusListBySourceName(String sourceName){
        String sql = String.format(queryReaderStatusList, sourceName);
        List<ReaderStatus> readerStatusList = ORMUtil.queryForList(sql, null, ReaderStatus.class);
        return readerStatusList;
    }

    public static void clearReaderStatus(String sourceName, String readerName){
        String sql = String.format(clearReaderStatus, sourceName, readerName);
        ORMUtil.executeSQL(sql, null);
    }

    public static ReaderStatus create(String sourceName, String readerName, int isFinished, int totalReader){

        ReaderStatus readerStatus = new ReaderStatus();
        readerStatus.setSourceName(sourceName);
        readerStatus.setReaderName(readerName);
        readerStatus.setIsFinished(isFinished);
        readerStatus.setTotalReader(totalReader);
        readerStatus.setGmtCreate(new Date());
        readerStatus.setGmtModified(new Date());
        return readerStatus;

    }
}
