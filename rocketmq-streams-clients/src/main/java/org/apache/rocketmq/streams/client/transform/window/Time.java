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

package org.apache.rocketmq.streams.client.transform.window;

/**
 * 给window指定窗口时间
 */
public class Time {
    protected int value;
    public Time(int value){
        this.value=value;
    }
    public static Time seconds(int second){

        return new Time(second);
    }
    public static Time minutes(int minutes){
        return new Time(minutes*60);
    }
    public static Time hours(int hours){
        return new Time(60*60*hours);
    }
    public static Time days(int days){
        return new Time(60*60*24*days);
    }

    public int getValue() {
        return value;
    }
}
