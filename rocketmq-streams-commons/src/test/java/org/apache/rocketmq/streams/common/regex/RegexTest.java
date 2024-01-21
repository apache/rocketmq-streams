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
package org.apache.rocketmq.streams.common.regex;

import com.google.re2j.Pattern;
import java.util.Set;
import org.apache.rocketmq.streams.common.optimization.RegexEngine;
import org.junit.Assert;
import org.junit.Test;

public class RegexTest {

    @Test
    public void testRe2Basic() {
        RegexEngine<Integer> regexEngine = new RegexEngine<>(true);
        regexEngine.addRegex("python", 0);
        regexEngine.addRegex("\\.dll", 1);
        String message = "python test.py";
        boolean matched = regexEngine.match(message);
        Assert.assertEquals(true, matched);
        message = "test.dll";
        Set<Integer> matchedSet = regexEngine.matchExpression(message);
        Assert.assertEquals(1, matchedSet.size());
        Assert.assertEquals(1, java.util.Optional.ofNullable(matchedSet.iterator().next()).get().intValue());
    }

    @Test
    public void testRe2UnSupport() {
        String regex = "(?P<P570>(^\\\\\"[^\\\\\"]+\\\\[^\\\\\\\\\"]*\\\\[^\\\\\\\\\"]*\\?\\w{3,4}(?=\\.)[^\\\\\\\\\"]*(?<=\\.)\\w{2,4}\\\\\"))";
        try {
            Pattern pattern = Pattern.compile(regex, 4);
        } catch (Exception e) {
            e.printStackTrace();
        }
        regex = "/(script|ttyrec|rootsh|sniffy|ttyrpld|ttysnoop|gmond)$";
        try {
            Pattern pattern = Pattern.compile(regex, 4);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
