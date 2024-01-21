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
package org.apache.rocketmq.streams.db.source;

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;
import org.apache.rocketmq.streams.common.configuration.ConfigurationKey;
import org.apache.rocketmq.streams.common.context.MessageOffset;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.connectors.model.PullMessage;
import org.apache.rocketmq.streams.connectors.reader.AbstractQueryReader;
import org.apache.rocketmq.streams.connectors.reader.ISplitReader;
import org.apache.rocketmq.streams.connectors.source.AbstractPullSource;
import org.apache.rocketmq.streams.connectors.source.AbstractQuerySource;
import org.apache.rocketmq.streams.db.driver.DriverBuilder;
import org.apache.rocketmq.streams.db.driver.JDBCDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBSource extends AbstractQuerySource {
    private static final Logger LOGGER = LoggerFactory.getLogger(DBSource.class);
    @ENVDependence protected String jdbcDriver = ConfigurationKey.DEFAULT_JDBC_DRIVER;
    @ENVDependence protected String url;
    @ENVDependence protected String userName;
    @ENVDependence protected String tableName; //指定要插入的数据表
    @ENVDependence protected String password;

    protected int batchSize=1000;

    protected String idField="id";
    protected String modifyTimeField;
    protected boolean isTimestamp=false;
    protected String modifyTimeFormat=DateUtil.DEFAULT_FORMAT;

    @Override protected ISplitReader createSplitReader(ISplit<?, ?> split) {
        return new AbstractQueryReader(split) {
            @Override public Iterator<PullMessage<?>> getMessage() {
                long endTime=startTime+dateAdd*60*1000;
                String startTimeForSQL=getTimeSQL(startTime);
                String endTimeForSQL=getTimeSQL(endTime);
                String sql="select * from `"+tableName+"` where `"+modifyTimeField+"`>="+startTimeForSQL+" and `"+modifyTimeField+"`<"+endTimeForSQL+" and `"+idField+"`>"+pageNum+" order by `"+idField+"` limit "+batchSize;
                JDBCDriver dataSource = DriverBuilder.createDriver(jdbcDriver, url, userName, password);
                List<Map<String, Object>> rows= dataSource.queryForList(sql);
                int rowSize=(rows==null||rows.size()==0)?0:rows.size();
                LOGGER.info("pull data from db:"+getTimeSQL(startTime)+"-"+getTimeSQL(endTime)+". pull data count is "+rowSize+"");
                if(rowSize==0){
                    startTime=startTime+pollingMinute*60*1000;
                    pageNum=pageInit;
                    return null;
                }
                List<PullMessage<?>> messages=new ArrayList<>();
                long maxIdValue=0;

                for(Map<String,Object> row:rows){
                    long idValue=Long.valueOf(row.get(idField).toString());
                    if(idValue>maxIdValue){
                        maxIdValue=idValue;
                    }
                    JSONObject msg=new JSONObject();
                    msg.putAll(row);
                    PullMessage pullMessage=new PullMessage();
                    pullMessage.setMessage(msg);
                    MessageOffset messageOffset=new MessageOffset(startTime);
                    messageOffset.addLayerOffset(idValue);
                    pullMessage.setMessageOffset(messageOffset);
                    messages.add(pullMessage);
                }
                if(rows.size()<batchSize){
                    startTime=startTime+pollingMinute*60*1000;
                    pageNum=0;
                }else {
                    pageNum=maxIdValue;
                }

                return messages.iterator();
            }
        };
    }

    protected String getTimeSQL(long time){
        if(isTimestamp){
            return time+"";
        }else {
            Date date = new Date(time);
            return "'"+DateUtil.format(date,modifyTimeFormat)+"'";
        }
    }
}
