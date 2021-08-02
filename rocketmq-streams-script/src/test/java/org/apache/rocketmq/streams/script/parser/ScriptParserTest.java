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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.rocketmq.streams.common.utils.ContantsUtil;
import org.junit.Test;

public class ScriptParserTest {

    @Test
    public void testContants() {
        String expression
            = "sign(data,',','datasourceid','logid','logtype','time','filename','keepfilename');sign(name6,'|');";
        Map<String, String> flags = new HashMap<>();
        String log = ContantsUtil.doConstantReplace(expression, flags, 1);
        Iterator<Entry<String, String>> it = flags.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, String> entry = it.next();
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }
        System.out.println(log);

    }
}
