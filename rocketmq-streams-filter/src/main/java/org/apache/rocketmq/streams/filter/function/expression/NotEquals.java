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

import org.apache.rocketmq.streams.filter.context.RuleContext;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.common.datatype.DateDataType;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionMethodAilas;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

import java.util.Date;

@Function

public class NotEquals extends CompareFunction {

    private final static double MIN_VALUE = 0.000001;
    private static final DateDataType dateDataType = new DateDataType(Date.class);

    @FunctionMethod(value = "!=", alias = "<>")
    @FunctionMethodAilas("不等于")
    @Override
    public Boolean doFunction(Expression expression, RuleContext context, Rule rule) {
        return super.doFunction(expression, context, rule);
    }

    public boolean compare(int a, int b) {
        return a != b;
    }

    public boolean compare(Integer a, Integer b) {
        return !a.equals(b);
    }

    public boolean compare(String a, Integer b) {
        try {
            Integer aa = new Integer(a);
            return !aa.equals(b);
        } catch (Exception e) {
            return false;
        }
    }

    public boolean compare(String a, String b) {
        a = FunctionUtils.getConstant(a);
        b = FunctionUtils.getConstant(b);
        return !a.equals(b);
    }

    public boolean compare(boolean a, boolean b) {
        return a != b;
    }

    public boolean compare(long a, long b) {
        return a != b;
    }

    public boolean compare(double a, double b) {
        if (a - b > -MIN_VALUE && a - b < MIN_VALUE) {
            return false;
        } else {
            return true;
        }

    }

    public boolean compare(float a, float b) {
        if (a - b > -MIN_VALUE && a - b < MIN_VALUE) {
            return true;
        } else {
            return false;
        }

    }

    public boolean compare(Boolean a, Boolean b) {
        return !a.equals(b);
    }

    public boolean compare(Long a, Long b) {
        return !a.equals(b);
    }

    public boolean compare(String a, Long b) {
        try {
            a = FunctionUtils.getConstant(a);
            Long aa = new Long(a);
            return !aa.equals(b);
        } catch (Exception e) {
            return false;
        }
    }

    public boolean compare(Double a, Double b) {
        if (a - b > -MIN_VALUE && a - b < MIN_VALUE) {
            return false;
        } else {
            return true;
        }

    }

    public boolean compare(String a, Double b) {
        try {
            a = FunctionUtils.getConstant(a);
            Double aa = new Double(a);
            if (aa - b > -MIN_VALUE && aa - b < MIN_VALUE) {
                return false;
            } else {
                return true;
            }
        } catch (Exception e) {
            return false;
        }
    }

    public boolean compare(Float a, Float b) {
        if (a - b > -MIN_VALUE && a - b < MIN_VALUE) {
            return true;
        } else {
            return false;
        }

    }

    public boolean compare(String a, Float b) {
        try {
            a = FunctionUtils.getConstant(a);
            Float aa = new Float(a);
            if (aa - b > -MIN_VALUE && aa - b < MIN_VALUE) {
                return false;
            } else {
                return true;
            }
        } catch (Exception e) {
            return false;
        }
    }

    public boolean compare(Date a, Date b) {
        String ad = dateDataType.toDataJson(a);
        String bd = dateDataType.toDataJson(b);
        return compare(ad, bd);
    }

    public static void main(String[] args) {
        Boolean a = true;
        Boolean b = false;
        NotEquals not = new NotEquals();
        System.out.println(not.compare(a, b));
    }
}
