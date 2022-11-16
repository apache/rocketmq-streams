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
package org.apache.rocketmq.streams.core.function.supplier;

import org.apache.rocketmq.streams.core.common.Constant;
import org.apache.rocketmq.streams.core.metadata.Data;
import org.apache.rocketmq.streams.core.running.AbstractProcessor;
import org.apache.rocketmq.streams.core.running.Processor;
import org.apache.rocketmq.streams.core.util.Utils;

import java.util.Properties;
import java.util.function.Supplier;

public class PrintSupplier<T> implements Supplier<Processor<T>> {


    @Override
    public Processor<T> get() {
        return new PrintProcessor<>();
    }

    static class PrintProcessor<T> extends AbstractProcessor<T> {


        public PrintProcessor() {
        }


        @Override
        public void process(T data) {
            Properties header = context.getHeader();
            Object startTime = header.get(Constant.WINDOW_START_TIME);
            Object endTime = header.get(Constant.WINDOW_END_TIME);
            if (startTime == null || endTime == null) {
                String template = "(key=%s, value=%s)";

                Data<Object, T> result = new Data<>(this.context.getKey(), data, this.context.getDataTime(), header);
                String format = String.format(template, result.getKey(), data.toString());

                System.out.println(format);
            } else {
                String template = "[%s - %s](key=%s, value=%s)";
                String start = Utils.format((Long)startTime);
                String end = Utils.format((Long)endTime);

                String format = String.format(template, start, end, this.context.getKey(), data);
                System.out.println(format);
            }


        }
    }

}
