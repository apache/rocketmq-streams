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
package org.apache.rocketmq.streams.filter.monitor;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.streams.filter.exception.RegexTimeoutException;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("rawtypes")
public abstract class Monitor<T> {

    protected long startTime;
    protected long endTime;
    protected String message;
    protected T result;
    protected boolean isException;
    protected Exception e;

    protected List<Monitor> children = new ArrayList<>();

    public long costTime() {
        // endTime=0时，Action还没结束，故不统计
        if (startTime <= 0 || endTime <= 0) {
            return 0;
        }
        return endTime - startTime;
    }

    public void begin(String message) {
        this.message = message;
        startTime = System.currentTimeMillis();
    }

    public void end() {
        endTime = System.currentTimeMillis();
    }

    public void end(T result) {
        end();
        if (result != null) {
            this.result = result;
        }
    }

    public void endException(Exception e) {
        end();
        this.e = e;
        this.isException = true;
    }

    public abstract String getType();

    public void addChildren(Monitor monitor) {
        children.add(monitor);
    }

    @Override
    public String toString() {
        JSONObject jsonObject = toJson();
        if (jsonObject == null) {
            return "";
        }
        return jsonObject.toJSONString();
    }

    public JSONObject toJson() {
        if (message == null) {
            return null;
        }
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("message", message);
        jsonObject.put("result", resultToString());
        jsonObject.put("cost_time", costTime());
        jsonObject.put("type", getType());
        if (children != null) {
            JSONArray jsonArray = new JSONArray();
            for (Monitor monitor : children) {
                jsonArray.add(monitor.toString());
            }
            jsonObject.put("children", jsonArray);
        }

        return jsonObject;
    }

    protected String resultToString() {
        if (result == null) {
            return "error";
        }
        return result.toString();
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public String getMessage() {
        return message;
    }

    public List<Monitor> getChildren() {
        return children;
    }

    public boolean isRegexTimeout() {
        if (!isException) {
            return false;
        }
        if (RegexTimeoutException.class.isInstance(e)) {
            return true;
        }
        return false;
    }

    public Exception getException() {
        return e;
    }

    public boolean isException() {
        return isException;
    }

    public T getResult() {
        return result;
    }

    public void setResult(T result) {
        this.result = result;
    }

}
