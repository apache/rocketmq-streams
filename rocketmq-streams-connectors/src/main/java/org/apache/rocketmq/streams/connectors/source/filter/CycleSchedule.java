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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;

/**
 * @description 用来做分区选取
 */
public class CycleSchedule implements Serializable {

    public static final int INIT_CYCLE_VERSION = 0;
    private static final long serialVersionUID = -5151597286296228754L;
    private static CycleSchedule INSTANCE;
    //历史数据读取时使用,表示比起当前相差多少个调度周期
    final long cycleDiff;
    CyclePeriod cyclePeriod;
    AtomicLong cycleId = new AtomicLong(0);
    String expression;
    boolean isInit;

    public CycleSchedule(String expr, Date date) throws ParseException {
        Date local = subMs(date);
        expression = expr;
        cycleId.set(INIT_CYCLE_VERSION);
        cyclePeriod = CyclePeriod.getInstance(expression);
        isInit = true;
        if (cyclePeriod.isHistory) {
            Date curCycleDateTime = calCycleDateTime(local);
            Date tmp = subMs(cyclePeriod.getHisDate());
            cycleDiff = curCycleDateTime.getTime() - tmp.getTime();
        } else {
            cycleDiff = 0;
        }
    }

    public static CycleSchedule getInstance(String expr, Date date) {
        if (INSTANCE == null) {
            synchronized (CycleSchedule.class) {
                if (INSTANCE == null) {
                    try {
                        INSTANCE = new CycleSchedule(expr, date);
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return INSTANCE;
    }

    /**
     * 去掉毫秒时间戳
     *
     * @param date
     * @return
     */
    private Date subMs(Date date) {
        long time = date.getTime() / 1000 * 1000;
        return new Date(time);
    }

    /**
     * @return 返回date格式的调度周期时间
     */
    private Date calCycleDateTime(Date date) {
        return cyclePeriod.format(date);
    }

    public Cycle nextCycle(Date date) {
        Date local = subMs(date);
        local = cyclePeriod.format(local);
        if (isInit) {
            isInit = false;
        } else {
            cycleId.incrementAndGet();
        }
        List<String> ret = calAllPattern(local);
        Cycle cycle = new Cycle();
        cycle.setCycleId(cycleId.get());
        cycle.setAllPattern(ret);
        cycle.setCycleDateStr(calCycleDateStr(local));
        cycle.setCycleCount(cyclePeriod.getCycle());
        cycle.setCurDateStr(cyclePeriod.getDateFormat().format(local));
        cycle.setCycleDiff(cycleDiff);
        return cycle;
    }

    private String calCycleDateStr(Date date) {
        long d = date.getTime() - cycleDiff;
        Date d1 = new Date(d);
        return cyclePeriod.getDateFormat().format(d1);
    }

    private List<String> calAllPattern(Date date) {
        List<String> allPatterns = new ArrayList<>();
        for (int i = 1; i <= cyclePeriod.getCycle(); i++) {
            long d = date.getTime() - i * cyclePeriod.getInterval() - cycleDiff;
            String s = cyclePeriod.getDateFormat().format(new Date(d));
            allPatterns.add(s);
        }
        return allPatterns;
    }

    public CyclePeriod getCyclePeriod() {
        return cyclePeriod;
    }

    public void setCyclePeriod(CyclePeriod cyclePeriod) {
        this.cyclePeriod = cyclePeriod;
    }

    public AtomicLong getCycleId() {
        return cycleId;
    }

    public void setCycleId(AtomicLong cycleId) {
        this.cycleId = cycleId;
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public boolean isInit() {
        return isInit;
    }

    public void setInit(boolean init) {
        isInit = init;
    }

    public long getCycleDiff() {
        return cycleDiff;
    }

    public static class Cycle extends BasedConfigurable implements Serializable {

        private static final long serialVersionUID = 4842560538716388622L;
        Long cycleId;
        List<String> allPattern;
        String cycleDateStr;
        Integer cycleCount;
        String curDateStr;
        long cycleDiff;

        public Cycle() {
        }

        public Integer getCycleCount() {
            return cycleCount;
        }

        public void setCycleCount(Integer cycleCount) {
            this.cycleCount = cycleCount;
        }

        public Long getCycleId() {
            return cycleId;
        }

        public void setCycleId(Long cycleId) {
            this.cycleId = cycleId;
        }

        public List<String> getAllPattern() {
            return allPattern;
        }

        public void setAllPattern(List<String> allPattern) {
            this.allPattern = allPattern;
        }

        public String getCycleDateStr() {
            return cycleDateStr;
        }

        public void setCycleDateStr(String cycleDateStr) {
            this.cycleDateStr = cycleDateStr;
        }

        public String getCurDateStr() {
            return curDateStr;
        }

        public void setCurDateStr(String curDateStr) {
            this.curDateStr = curDateStr;
        }

        public long getCycleDiff() {
            return cycleDiff;
        }

        public void setCycleDiff(long cycleDiff) {
            this.cycleDiff = cycleDiff;
        }

        @Override
        public String toString() {
            return "Cycle{" +
                "cycleId=" + cycleId +
                ", cycleDateStr='" + cycleDateStr + '\'' +
                ", cycleCount=" + cycleCount +
                ", curDateStr='" + curDateStr + '\'' +
                ", cycleDiff=" + cycleDiff +
                ", allPattern=" + Arrays.toString(allPattern.toArray()) +
                '}';
        }
    }

}
