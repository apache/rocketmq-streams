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

import org.apache.rocketmq.streams.script.service.IScriptExpression;

/**
 * 完成脚本解析，支持函数嵌套，需要考虑特殊字符，如常量中的各种字符 解析思路：先做常量替换，把常量替换成固定字符串，再做ifelse解析，最后按；分隔
 */
public interface IScriptExpressionParser {

    /**
     * 把一个scriptStr字符串解析称一个IScriptItem 对象
     *
     * @param scriptStr
     * @return
     */
    IScriptExpression parse(String scriptStr);

    /**
     * 这个解析器是否支持这种解析
     *
     * @param itemStr
     * @return
     */
    boolean support(String itemStr);
}
