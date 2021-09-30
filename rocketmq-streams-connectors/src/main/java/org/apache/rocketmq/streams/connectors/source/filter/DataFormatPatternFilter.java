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
package org.apache.rocketmq.streams.connectors.source.filter;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * @description
 */
public class DataFormatPatternFilter extends AbstractPatternFilter implements Serializable {

    private static final long serialVersionUID = 3604787588465242642L;

    static final Log logger = LogFactory.getLog(DataFormatPatternFilter.class);


    static final String yyyyMMddHHmmss = "yyyyMMddHHmmss";
    static final String yyyyMMdd = "yyyyMMdd";
    static final String yyyyMMddHH = "yyyyMMddHH";

    SimpleDateFormat format1 = new SimpleDateFormat(yyyyMMdd);
    SimpleDateFormat format2 = new SimpleDateFormat(yyyyMMddHH);
    SimpleDateFormat format3 = new SimpleDateFormat(yyyyMMddHHmmss);

    @Override
    public boolean filter(String sourceName, String logicTableName,  String tableNameSuffix) {

        int len = tableNameSuffix.length();
        boolean isFilter = false;

        switch (len){
            case 8:
                try {
                    format1.parse(tableNameSuffix);
                    isFilter = true;
                } catch (ParseException e) {
                    e.printStackTrace();
                    isFilter = false;
                }
                break;
            case 10:
                try {
                    format2.parse(tableNameSuffix);
                    isFilter = true;
                } catch (ParseException e) {
                    e.printStackTrace();
                    isFilter = false;
                }
                break;
            case 14:
                try {
                    format3.parse(tableNameSuffix);
                    isFilter = true;
                } catch (ParseException e) {
                    e.printStackTrace();
                    isFilter = false;
                }
                break;
        }

        if(isFilter){
            logger.info(String.format("filter sourceName %s, logicTableName %s, suffix %s", sourceName, logicTableName, tableNameSuffix));
            return true;
        }
        if(next != null){
            return next.filter(sourceName,logicTableName, tableNameSuffix);
        }
        return false;
    }

    @Override
    public PatternFilter setNext(PatternFilter filter) {
        super.setNext(filter);
        return this;
    }

    public PatternFilter getNext() {
        return next;
    }

    public static void main(String[] args){
        DataFormatPatternFilter filter = new DataFormatPatternFilter();
//        System.out.println(filter.filter("20200101"));
//        System.out.println(filter.filter("2020010101"));
//        System.out.println(filter.filter("20200101010101"));

    }

}
