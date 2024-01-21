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

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @description 用来做分区选取
 */
public class CyclePatternFilter extends AbstractPatternFilter implements Serializable {

    public static final int INIT_CYCLE_VERSION = 0;
    private static final long serialVersionUID = -5151597286296228754L;
    //历史数据读取时使用,表示比起当前相差多少个调度周期
    final long cycleDiff;
    CyclePeriod cyclePeriod;
    Date curCycleDateTime; //当前调度周期时间
    long cycleId;
    String firstStartTime; //当前最小时间
    List<String> allPatterns;
    String expression;
    boolean isInit;

    //todo expr解析
    public CyclePatternFilter(String expr, Date date) throws ParseException {
        expression = expr;
        cycleId = INIT_CYCLE_VERSION;
        cyclePeriod = CyclePeriod.getInstance(expression);
        curCycleDateTime = calCycleDateTime(date);
        allPatterns = new ArrayList<>();
        isInit = true;
        if (cyclePeriod.isHistory) {
            Date tmp = cyclePeriod.getHisDate();
            cycleDiff = curCycleDateTime.getTime() / 1000 * 1000 - tmp.getTime() / 1000 * 1000;
        } else {
            cycleDiff = 0;
        }
    }

    public static void main(String[] args) throws ParseException {

        CyclePatternFilter cycle = new CyclePatternFilter("yyyyMMddHHmm - 15m", new Date());
        System.out.println(cycle);

        System.out.println(cycle.filter(null, null, "202109131650"));
        System.out.println(cycle.filter(null, null, "20210902000000"));
        System.out.println(cycle.filter(null, null, "20210908000000"));
        System.out.println(cycle.filter(null, null, "20210910000000"));
        System.out.println(cycle.filter(null, null, "20210909230000"));

        System.out.println(new SimpleDateFormat("yyyyMMddHH").parse("2021090923"));
        System.out.println(new SimpleDateFormat("yyyyMMddhhmmss").parse("20210909230000"));
        System.out.println(new SimpleDateFormat("yyyyMMddHHmmss").parse("20210909100000"));
        System.out.println(new SimpleDateFormat("yyyyMMddhhmmss").parse("20210909100000"));

    }

    /**
     * @return 返回date格式的调度周期时间
     */
    private Date calCycleDateTime(Date date) {
        return cyclePeriod.format(date);
    }

    private long calCycle(Date date) {
        Date tmp = calCycleDateTime(date);
        if (tmp.getTime() / 1000 == curCycleDateTime.getTime() / 1000) {
            return cycleId;
        }
        return nextCycle(tmp);
    }

    private long nextCycle(Date date) {
        curCycleDateTime = date;
        cycleId++;
        calAllPattern();
        return cycleId;
    }

    private void calAllPattern() {
        allPatterns.clear();
        for (int i = 1; i <= cyclePeriod.getCycle(); i++) {
            long d = (curCycleDateTime.getTime() / 1000) * 1000 - i * cyclePeriod.getInterval() - cycleDiff;
            String s = cyclePeriod.getDateFormat().format(new Date(d));
            allPatterns.add(s);
        }
        firstStartTime = allPatterns.get(allPatterns.size() - 1);
    }

    public boolean isNextCycle(Date date) {
        if (isInit) {
            isInit = false;
            calAllPattern();
            return true;
        }
        long tmp = cycleId;
        return calCycle(date) > tmp;
    }

    public List<String> getAllPatterns() {
        return allPatterns;
    }

    public long getCycleId() {
        return cycleId;
    }

    public Date getCurCycleDateTime() {
        return curCycleDateTime;
    }

    public String getCurCycleDateTimeStr() {
        return cyclePeriod.getDateFormat().format(curCycleDateTime);
    }

    public long getCycleDiff() {
        return cycleDiff;
    }

    public long getCyclePeriodDiff() {
        return cycleDiff / cyclePeriod.getInterval();
    }

    public int getCycle() {
        return cyclePeriod.getCycle();
    }

    public String getFirstStartTime() {
        return firstStartTime;
    }

    @Override
    public boolean filter(String sourceName, String logicTableName, String tableName) {
        return allPatterns.contains(tableName);
    }

}
