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
package org.apache.rocketmq.streams.script.parser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.rocketmq.streams.script.parser.imp.ConditionExpressionParser;
import org.apache.rocketmq.streams.script.parser.imp.FunctionExpressionParser;
import org.apache.rocketmq.streams.script.service.IScriptExpression;
import org.apache.rocketmq.streams.script.utils.ScriptParserUtil;

public class ScriptExpressionParserFactory implements IScriptExpressionParser {

    private static ScriptExpressionParserFactory INSTANCE;

    private static List<IScriptExpressionParser> parserList = new ArrayList<>();

    private ScriptExpressionParserFactory() {
    }

    static {
        INSTANCE = new ScriptExpressionParserFactory();
        parserList.add(new ConditionExpressionParser("if", "{", "else"));
        parserList.add(new ConditionExpressionParser("case", "{", "then"));
        parserList.add(new FunctionExpressionParser());
    }

    public static ScriptExpressionParserFactory getInstance() {
        return INSTANCE;
    }

    @Override
    public IScriptExpression parse(String itemStr) {
        Map<String, String> flag2ExpressionStr = new HashMap<>();
        String expressionStr = ScriptParserUtil.doConstantReplace(itemStr, flag2ExpressionStr, 1);
        for (IScriptExpressionParser parser : parserList) {
            if (parser.support(expressionStr)) {
                return parser.parse(itemStr);
            }
        }
        return null;
    }

    @Override
    public boolean support(String itemStr) {
        for (IScriptExpressionParser parser : parserList) {
            if (parser.support(itemStr)) {
                return true;
            }
        }
        return false;
    }
}
