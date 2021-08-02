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

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.SimpleLayout;

public class LoggerCreator {

    /**
     * 根据名字创建一个不同logger，解决多个name输出多个文件的场景
     *
     * @param name
     * @param filePath
     * @param level
     * @return
     */
    public static Logger create(String name, String filePath, Level level) {
        Logger logger = Logger.getLogger(name);
        try {
            SimpleLayout simpleLayout = new SimpleLayout();
            MyDailyRollingFileAppender fa = new MyDailyRollingFileAppender();
            fa.setFile(filePath);
            fa.setAppend(true);
            fa.setMaxBackupIndex(7);
            fa.setEncoding("UTF-8");
            fa.setDatePattern("'.'yyyy-MM-dd");

            PatternLayout patternLayout = new PatternLayout("%d [%-5p]-[%t]-[%c{1}] (%F:%L) %m%n");
            fa.setLayout(patternLayout);

            fa.activateOptions();

            logger.addAppender(fa);
            logger.setLevel(level);
        } catch (Exception e) {
            throw new RuntimeException("create log error, the output is " + filePath + ", the name is " + name + ", the lever is " + level, e);
        }
        return logger;
    }
}
