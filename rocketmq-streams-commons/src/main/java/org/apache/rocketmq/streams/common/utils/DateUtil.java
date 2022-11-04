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
package org.apache.rocketmq.streams.common.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public final class DateUtil {

    public static final String MINUTE_FORMAT = "HH:mm";
    public static final String SHORT_DAY_FORMAT = "MM-dd";
    public static final String DAY_FORMAT = "yyyy-MM-dd";
    public static final String SIMPLE_DAY_FORMAT = "yyyyMMdd";
    public static final String DEFAULT_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String SIMPLE_SECOND_FORMAT = "yyyyMMddHHmmss";
    public static final String TIMESTAMP_FORMAT = "yyyyMMddHHmmssSSS";
    private static final ThreadLocal<Map<String, DateFormat>> tl = new ThreadLocal<Map<String, DateFormat>>();


    /**
     * 给指定时间增加一个时间值
     *
     * @param timeUnit
     * @param date
     * @param value
     * @return
     */
    public static Date addDate(TimeUnit timeUnit, Date date, int value) {
        if (TimeUnit.SECONDS == timeUnit) {
            return DateUtil.addSecond(date, value);
        } else if (TimeUnit.MINUTES == timeUnit) {
            return DateUtil.addMinute(date, value);
        } else if (TimeUnit.HOURS == timeUnit) {
            return DateUtil.addHour(date, value);
        } else if (TimeUnit.DAYS == timeUnit) {
            return DateUtil.addDays(date, value);
        }
        return null;
    }

    public static String getTimeStampAndUUIDString(Date date) {
        return getDateString(date, TIMESTAMP_FORMAT) + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    public static String getDateString(Date date, String format) {
        return date == null ? null : new SimpleDateFormat(format).format(date);
    }

    public static Date addMinute(Date date, int minutes) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.MINUTE, minutes);
        return calendar.getTime();
    }

    public static Date addSecond(Date date, int seconds) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.SECOND, seconds);
        return calendar.getTime();
    }

    public static Date addHour(Date date, int hours) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.HOUR_OF_DAY, hours);
        return calendar.getTime();
    }

    public static Date addDays(Date date, int days) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.DAY_OF_YEAR, days);
        return calendar.getTime();
    }

    public static Date addMonths(Date date, int months) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.MONTH, months);
        return calendar.getTime();
    }

    public static Date addYears(Date date, int years) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.YEAR, years);
        return calendar.getTime();
    }

    public static boolean equals(Date d1, Date d2) {
        if (d1 == null) {
            return d2 == null;
        }

        if (d2 == null) {
            return false;
        }

        return d1.getTime() == d2.getTime();
    }

    public static final long DAY_MILLISECONDS = 24 * 60 * 60 * 1000;

    public static int dayDiff(Date d1, Date d2) {
        long timeDiff = d1.getTime() - d2.getTime();
        if (timeDiff < 0) {
            timeDiff = -timeDiff;
        }
        return (int) (timeDiff / DAY_MILLISECONDS);
    }

    public static long dateDiff(Date d1, Date d2) {
        long timeDiff = d1.getTime() - d2.getTime();
        return timeDiff;
    }

    public static int monthDiff(Date d1, Date d2) {
        Calendar c1 = Calendar.getInstance();
        c1.setTime(d1);
        int m1 = c1.get(Calendar.MONTH);
        int y1 = c1.get(Calendar.YEAR);

        Calendar c2 = Calendar.getInstance();
        c2.setTime(d2);
        int m2 = c2.get(Calendar.MONTH);
        int y2 = c2.get(Calendar.YEAR);

        int diff = (y1 - y2) * 12 + (m1 - m2);
        if (diff < 0) {
            diff = -diff;
        }
        return diff;
    }

    public static int yearDiff(Date d1, Date d2) {
        Calendar c1 = Calendar.getInstance();
        c1.setTime(d1);
        int y1 = c1.get(Calendar.YEAR);

        Calendar c2 = Calendar.getInstance();
        c2.setTime(d2);
        int y2 = c2.get(Calendar.YEAR);

        int diff = (y1 - y2);
        if (diff < 0) {
            diff = -diff;
        }
        return diff;
    }

    public static String format(Date date) {
        return format(date, DEFAULT_FORMAT);
    }

    public static String dateToString(Date date) {
        try {
            return format(date, DEFAULT_FORMAT);
        } catch (Throwable th) {
            return "";
        }
    }

    public static String format(Date date, String pattern) {
        // Assert.notNull(date, "date = null");
        // Assert.hasLength(pattern, "pattern is empty");
        if (date == null) {
            return null;
        }
        DateFormat sdf = getDateFormat(pattern);
        return sdf.format(date);
    }

    public static Date parse(String dateStr) {
        return parse(dateStr, DEFAULT_FORMAT);
    }

    public static Date parse(String dateStr, String format) {
        if (StringUtil.isEmpty(dateStr)) {
            return null;
        }
        DateFormat sdf = getDateFormat(format);
        try {
            return sdf.parse(dateStr);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static DateFormat getDateFormat(String format) {
        Map<String, DateFormat> map = tl.get();

        if (map == null) {
            map = new HashMap<String, DateFormat>();
            tl.set(map);
        }

        if (StringUtil.isEmpty(format)) {
            format = DEFAULT_FORMAT;
        }

        DateFormat ret = map.get(format);
        if (ret == null) {
            ret = new SimpleDateFormat(format);
            map.put(format, ret);
        }

        return ret;
    }

    public static Date getDateOfToday() {
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTime();
    }

    /**
     * Get Default DateFormat
     *
     * @return
     */
    public static DateFormat getDateFormat() {
        return getDateFormat(null);
    }

    public static boolean isFirstDayOfMonth(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.DAY_OF_MONTH) == 1;
    }

    public static Date getFirstDayOfMonth(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        return calendar.getTime();
    }

    public static int getDayOfWeek(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.DAY_OF_WEEK);
    }

    public static int getWeekOfYear(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setFirstDayOfWeek(Calendar.MONDAY);
        calendar.setTime(date);
        return calendar.get(Calendar.WEEK_OF_YEAR);
    }

    private static final int WORKTIME_DAY_OF_WEEK_BEGIN = 1;
    private static final int WORKTIME_DAY_OF_WEEK_END = 5;
    private static final int WORKTIME_HOUR_OF_DAY_BEGIN = 9;
    private static final int WORKTIME_HOUR_OF_DAY_END = 18;

    /**
     * 判断是否是工作时间，工作时间为周一到周五（9:00-18:00）
     *
     * @param date
     * @return
     */
    public static boolean isInWorkTime(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        int day = calendar.get(Calendar.DAY_OF_WEEK) - 1;
        if (day < WORKTIME_DAY_OF_WEEK_BEGIN || day > WORKTIME_DAY_OF_WEEK_END) {
            return false;
        }
        int hour = calendar.get(Calendar.HOUR_OF_DAY);
        if (hour < WORKTIME_HOUR_OF_DAY_BEGIN || hour > WORKTIME_HOUR_OF_DAY_END) {
            return false;
        }
        return true;
    }

    public static boolean isValidTime(String string) {
        return isValidTime(string, DEFAULT_FORMAT);
    }

    public static boolean isValidTime(String string, String format) {
        if (StringUtil.isEmpty(string)) {
            return false;
        }

        string = string.trim();
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        try {
            Date date = sdf.parse(string);
            return string.equals(sdf.format(date));
        } catch (Throwable th) {
        }
        return false;
    }

    public static Date parseTime(String string) {
        if (null == string || "".equals(string)) {
            return null;
        }

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            return sdf.parse(string);
        } catch (Throwable th) {
        }
        return null;
    }

    public static Date parseTime(String string, String format) {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        try {
            return sdf.parse(string);
        } catch (Throwable th) {
        }
        return null;
    }

    public static String getAfterMinutesTime(String time, int minutes) {
        Calendar c = Calendar.getInstance();
        c.setTime(DateUtil.parseTime(time));
        c.add(Calendar.MINUTE, minutes);
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return f.format(c.getTime());
    }

    public static String getBeforeMinutesTime(String time, int minutes) {
        Calendar c = Calendar.getInstance();
        c.setTime(DateUtil.parseTime(time));
        c.add(Calendar.MINUTE, -minutes);
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return f.format(c.getTime());
    }

    public static Date getBeforeMinutesTime(int minutes) {
        Calendar c = Calendar.getInstance();
        c.add(Calendar.MINUTE, -minutes);
        return c.getTime();
    }

    public static Date getBeforeHour(int hour) {
        Calendar c = Calendar.getInstance();
        c.add(Calendar.HOUR, -hour);
        return c.getTime();
    }

    public static String getCurrentTimeString() {
        Calendar c = Calendar.getInstance();
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return f.format(c.getTime());
    }

    public static String getCurrentTimeString(String format) {
        if (null == format) {
            return getCurrentTimeString();
        }
        Calendar c = Calendar.getInstance();
        SimpleDateFormat f = new SimpleDateFormat(format);
        return f.format(c.getTime());
    }

    public static Date getCurrentTime() {
        Calendar c = Calendar.getInstance();
        return c.getTime();
    }

    /**
     * 获取date所在的当天起始的时间
     *
     * @param date
     * @return
     */
    public static Date getBeginOfDate(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTime();
    }

    public static Date getBeginOfHour(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTime();
    }

    public static Date getBeginOfMinute(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTime();
    }

    public static long getBeginTimestampOfMinute(long time) {
        Date d = new Date(time);
        return getBeginOfMinute(d).getTime();
    }

    public static Date getBeginOfSecond(Date date, int deltaSec) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        int sec = calendar.get(Calendar.SECOND); // 取秒
        int newSec = (sec / deltaSec) * deltaSec;
        calendar.set(Calendar.SECOND, newSec);
        return calendar.getTime();
    }

    public static List<Date> getWindowBeginTime(long eventTime, long slideInterval, long sizeInterval) {
        if (eventTime > Long.MIN_VALUE) {
            List<Date> hitList = new ArrayList<>((int) (sizeInterval / slideInterval));
            long lastStart = getWindowStartWithOffset(eventTime, 0, slideInterval);
            for (long start = lastStart; start > eventTime - sizeInterval; start -= slideInterval) {
                hitList.add(new Date(start));
            }
            return hitList;
        } else {
            throw new RuntimeException("illegal event time! " + eventTime);
        }
    }

    public static Date getWindowBeginTime(long eventTime,long sizeInterval) {
        return getWindowBeginTime(eventTime,sizeInterval,sizeInterval).get(0);
    }


    /**
     * get the window start for a timestamp.
     *
     * @param timestamp  epoch millisecond to get the window start.
     * @param offset     The offset which window start would be shifted by. mostly used for special time, such as every 15th minutes of one hour, or UTC time
     * @param windowSize The size of the generated windows.
     * @return window start
     */
    public static long getWindowStartWithOffset(long timestamp, long offset, long windowSize) {
        return timestamp - (timestamp - offset + windowSize) % windowSize;
    }

    public static boolean isToday(Date date) {
        long dayTime = date.getTime() / DAY_MILLISECONDS;
        long today = System.currentTimeMillis() / DAY_MILLISECONDS;
        return dayTime == today;
    }

    public static boolean before(Date d1, Date d2) {
        return d1.getTime() < d2.getTime();
    }

    public static boolean isSameDay(Date date1, Date date2) {
        if (date1 == null || date2 == null) {
            return false;
        }
        Calendar calendar1 = Calendar.getInstance();
        calendar1.setTime(date1);
        Calendar calendar2 = Calendar.getInstance();
        calendar2.setTime(date2);
        if (calendar1.get(Calendar.YEAR) != calendar2.get(Calendar.YEAR)) {
            return false;
        }
        if (calendar1.get(Calendar.MONTH) != calendar2.get(Calendar.MONTH)) {
            return false;
        }
        if (calendar1.get(Calendar.DAY_OF_MONTH) != calendar2.get(Calendar.DAY_OF_MONTH)) {
            return false;
        }
        return true;
    }

    public static Date getHourDate(Date date, int hour) {
        if (hour < 0 || hour > 24) {
            throw new IllegalArgumentException("hour must be in 0 ~ 24");
        }

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.HOUR_OF_DAY, hour);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTime();
    }

    public static int getHour(Date date) {

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.HOUR_OF_DAY);
    }

    /**
     * 获取日期年份
     *
     * @param date 日期
     * @return
     */
    public static String getYear(Date date) {
        return format(date).substring(0, 4);
    }

    /**
     * 功能描述：返回月
     *
     * @param date Date 日期
     * @return 返回月份
     */
    public static int getMonth(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.MONTH) + 1;
    }

    /**
     * 功能描述：返回日
     *
     * @param date Date 日期
     * @return 返回日份
     */
    public static int getDay(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.DAY_OF_MONTH);
    }

    /**
     * 功能描述：返回分
     *
     * @param date 日期
     * @return 返回分钟
     */
    public static int getMinute(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.MINUTE);
    }

    /**
     * 返回秒钟
     *
     * @param date Date 日期
     * @return 返回秒钟
     */
    public static int getSecond(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.SECOND);
    }

    public static String longToString(long time) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat(DEFAULT_FORMAT);
            Date date = new Date(time);
            return sdf.format(date);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String longToSimpleString(long time) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat(SIMPLE_SECOND_FORMAT);
            Date date = new Date(time);
            return sdf.format(date);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static long StringToLong(String datestr) {
        try {
            Date d = DateUtil.parse(datestr);
            return d.getTime();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage() + " ---------" + datestr, e);
        }
    }

}
