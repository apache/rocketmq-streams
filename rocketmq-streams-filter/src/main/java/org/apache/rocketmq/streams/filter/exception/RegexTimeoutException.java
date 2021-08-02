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
package org.apache.rocketmq.streams.filter.exception;

public class RegexTimeoutException extends RuntimeException {
    private String regex;
    private String context;
    private long timeout;

    public RegexTimeoutException(String message) {
        super(message);
    }

    public RegexTimeoutException(String regex, String context, long timeout) {
        // super("正则表达式执行超时，超时的正则为："+regex+"。匹配的内容为："+context+"。对应的超时时间为："+timeout);
        super("The regular expression executes timeout, and the timeout is:" + regex + ". The matching content is "
            + context + ". The corresponding timeout time is: " + timeout);
        this.regex = regex;
        this.context = context;
        this.timeout = timeout;
    }

    public String getRegex() {
        return regex;
    }

    public String getContext() {
        return context;
    }

    public long getTimeout() {
        return timeout;
    }
}
