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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @Description
 */
public enum CyclePeriod {

    CYCLE_PERIOD_DATE(){

        @Override
        void argsParser(String expr) throws ParseException {
            super.argsParser(expr);
            interval = 24 * 3600 * 1000;
            int length = expr.length();
            if(length == 8 && checkFormat(expr, PatternFilter.yyyyMMdd)){
                format = PatternFilter.yyyyMMdd;
            }else if(length == 14 && checkFormat(expr, PatternFilter.yyyyMMddHHmmss)){
                format = PatternFilter.yyyyMMddHHmmss;
            }else{
                throw new RuntimeException(String.format("unsupported format : %s, only support yyyymmdd 、 yyyymmddhhmmss.", expr));
            }
        }

        @Override
        Date format(Date strDate){
            Date date = new Date(strDate.getTime());
            date.setHours(0);
            date.setMinutes(0);
            date.setSeconds(0);
            return date;
        }

    },
    CYCLE_PERIOD_HOUR(){

        @Override
        void argsParser(String expr) throws ParseException {
            super.argsParser(expr);
            interval = 3600 * 1000;

            int length = expr.length();
            if(length == 10 && checkFormat(expr, PatternFilter.yyyyMMddHH)){
                format = PatternFilter.yyyyMMddHH;
            }else if(length == 14 && checkFormat(expr, PatternFilter.yyyyMMddHHmmss)){
                format = PatternFilter.yyyyMMddHHmmss;
            }else{
                throw new RuntimeException(String.format("unsupported format : %s, only support yyyymmdd 、 yyyymmddhhmmss.", expr));
            }
        }

        @Override
        Date format(Date strDate){
            Date date = new Date(strDate.getTime());
            date.setMinutes(0);
            date.setSeconds(0);
            return date;
        }

    },
    CYCLE_PERIOD_MINUTE(){

        @Override
        void argsParser(String expr) throws ParseException {
            super.argsParser(expr);
            interval = 60 * 1000;
            int length = expr.length();
            if(length == 12 && checkFormat(expr, PatternFilter.yyyyMMddHHmm)){
                format = PatternFilter.yyyyMMddHHmm;
            }else if(length == 14 && checkFormat(expr, PatternFilter.yyyyMMddHHmmss)){
                format = PatternFilter.yyyyMMddHHmmss;
            }else{
                throw new RuntimeException(String.format("unsupported format : %s, only support yyyymmdd 、 yyyymmddhhmmss.", expr));
            }
        }

        @Override
        Date format(Date strDate){
            Date date = new Date(strDate.getTime());
            date.setSeconds(0);
            return date;
        }

    }
    ;

    boolean isHistory = false;

    long interval;

    int cycle;

    String format;

    String hisDateString;

    static final Log logger = LogFactory.getLog(CyclePeriod.class);

    void argsParser(String expr) throws ParseException {
        if(expr.matches("^\\d+$")){
            isHistory = true;
            hisDateString = expr;
        }
    }

    Date format(Date strDate){
        throw new RuntimeException(String.format("unsupported type.", strDate));
    }

    /**
     * expr可能是yyyymmdd 或者 20210917
     * @param expr
     * @param format
     * @return
     */
    final boolean checkFormat(String expr, String format){

        if(!isHistory){
            return expr.equalsIgnoreCase(format);
        }

        try {
            new SimpleDateFormat(format).parse(expr);
            return true;
        } catch (ParseException e) {
            logger.error(String.format("error format, expr is %s, format is %s.", expr, format));
            e.printStackTrace();
            return false;
        }
    }

    public Date getHisDate() throws ParseException {
        return getDateFormat().parse(hisDateString);
    }

    public SimpleDateFormat getDateFormat(){
        return new SimpleDateFormat(format);
    }

    public long getInterval(){
        return interval;
    }

    public boolean isHistory() {
        return isHistory;
    }

    public void setHistory(boolean history) {
        isHistory = history;
    }

    public void setInterval(long interval) {
        this.interval = interval;
    }



    public int getCycle() {
        return cycle;
    }

    public void setCycle(int cycle) {
        this.cycle = cycle;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getFormat() {
        return format;
    }

    public String getHisDateString() {
        return hisDateString;
    }

    public void setHisDateString(String hisDateString) {
        this.hisDateString = hisDateString;
    }


    public static CyclePeriod getInstance(String expression) throws ParseException {

        String[] str = expression.split("\\-");
        assert str.length == 2 : String.format("expression error : %s. ", expression);
        String expr = str[0].trim();
        String tmp = str[1].trim().toLowerCase();
        String cycleStr = tmp.substring(0, tmp.length() - 1);
        int cycle = Integer.parseInt(cycleStr);
        CyclePeriod cyclePeriod = null;
        if(tmp.endsWith("d")){
            cyclePeriod = CYCLE_PERIOD_DATE;
        }else if(tmp.endsWith("h")){
            cyclePeriod = CYCLE_PERIOD_HOUR;
        }else if(tmp.endsWith("m")){
            cyclePeriod = CYCLE_PERIOD_MINUTE;
        }else{
            new RuntimeException(String.format("unsupported format : %s", expression));
        }
        cyclePeriod.argsParser(expr);
        cyclePeriod.cycle = cycle;

        return cyclePeriod;
    }



}
