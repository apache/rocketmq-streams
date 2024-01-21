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
package org.apache.rocketmq.streams.common.logger;

import com.alibaba.fastjson.JSONObject;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.rocketmq.streams.common.channel.sink.AbstractSink;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.monitor.IMonitor;
import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.apache.rocketmq.streams.common.utils.JsonableUtil;
import org.apache.rocketmq.streams.common.utils.PrintUtil;

public class LoggerOutputChannel extends AbstractSink {
    private static transient Map<String, Logger> loggerFactory = new HashMap<>();
    protected transient Logger logger;

    public LoggerOutputChannel(String dir, String logName) {
        this.logger = loggerFactory.get(logName);
        File file = new File(dir);
        if (file.exists() == false) {
            file.mkdirs();
        }
        if (logger == null) {
            synchronized (IMonitor.class) {
                logger = loggerFactory.get(logName);
                if (logger == null) {
                    String filePath = FileUtil.concatFilePath(dir, logName);
                    logger = LoggerCreator.create(logName, filePath, Level.INFO);
                    loggerFactory.put(logName, logger);
                }
            }
        }
    }

    @Override
    protected boolean batchInsert(List<IMessage> messages) {
        if (messages == null || messages.size() == 0) {
            return true;
        }
        StringBuilder stringBuilder = new StringBuilder();
        boolean isFirst = true;
        for (IMessage msg : messages) {
            String result = msg.getMessageValue().toString();
            if (JSONObject.class.isInstance(msg.getMessageValue())) {
                result = JsonableUtil.formatJson((JSONObject) msg.getMessageValue());
            }
            if (isFirst) {
                isFirst = false;
            } else {
                stringBuilder.append(PrintUtil.LINE);
            }
            stringBuilder.append(result);
        }
        logger.info(stringBuilder.toString());
        return true;
    }

}
