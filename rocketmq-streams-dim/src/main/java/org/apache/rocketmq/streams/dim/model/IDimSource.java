package org.apache.rocketmq.streams.dim.model;

import java.util.List;

/**
 * @author zengyu.cw
 * @program rocketmq-streams-apache
 * @create 2021-11-17 09:42:20
 * @description
 */
public interface IDimSource<InputRecord> {

    public boolean put(InputRecord record) throws Exception;

    public int batchPut(List<InputRecord> recordList) throws Exception;

    public IDataCache initCache();

    public boolean buildIndex(IDataCache cache);

}
