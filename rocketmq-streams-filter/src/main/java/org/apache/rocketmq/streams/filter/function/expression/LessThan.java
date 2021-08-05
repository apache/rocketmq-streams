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
package org.apache.rocketmq.streams.filter.function.expression;

import java.util.Date;
import org.apache.rocketmq.streams.common.datatype.DateDataType;
import org.apache.rocketmq.streams.filter.context.RuleContext;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionMethodAilas;

@Function

public class LessThan extends CompareFunction {

    private static final DateDataType dateDataType = new DateDataType(Date.class);

    @Override
    @FunctionMethod(value = "&lt;", alias = "<")
    @FunctionMethodAilas("小于")
    public Boolean doFunction(Expression expression, RuleContext context, Rule rule) {
        return super.doFunction(expression, context, rule);
    }

    public boolean compare(int a, int b) {
        return a < b;
    }

    public boolean compare(Integer a, Integer b) {
        return a < b;
    }

    public boolean compare(String a, Integer b) {
        try {
            Integer aa = new Integer(a);
            return aa < b;
        } catch (Exception e) {
            return false;
        }
    }

    public boolean compare(String a, String b) {
        return a.compareTo(b) < 0;
    }

    public boolean compare(long a, long b) {
        return a < b;
    }

    public boolean compare(double a, double b) {
        return a - b < 0;
    }

    public boolean compare(float a, float b) {
        return a - b < 0;
    }

    public boolean compare(Long a, Long b) {
        return a < b;
    }

    public boolean compare(String a, Long b) {
        try {
            Long aa = new Long(a);
            return aa < b;
        } catch (Exception e) {
            return false;
        }
    }

    public boolean compare(Double a, Double b) {
        return a - b < 0;
    }

    public boolean compare(String a, Double b) {
        try {
            Double aa = new Double(a);
            return aa - b < 0;
        } catch (Exception e) {
            return false;
        }
    }

    public boolean compare(Float a, Float b) {
        return a - b < 0;
    }

    public boolean compare(String a, Float b) {
        try {
            Float aa = new Float(a);
            return aa - b < 0;
        } catch (Exception e) {
            return false;
        }
    }

    public boolean compare(Date a, Date b) {
        String ad = dateDataType.toDataJson(a);
        String bd = dateDataType.toDataJson(b);
        return compare(ad, bd);
    }
}
