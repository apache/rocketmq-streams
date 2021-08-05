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
package org.apache.rocketmq.streams.common.optimization;

import com.gliwka.hyperscan.wrapper.Database;
import com.gliwka.hyperscan.wrapper.Expression;
import com.gliwka.hyperscan.wrapper.ExpressionFlag;
import com.gliwka.hyperscan.wrapper.Match;
import com.gliwka.hyperscan.wrapper.Scanner;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class HyperscanRegex<T> {
    protected List<Expression> regexs = new ArrayList<>();
    protected Database db;
    protected Scanner scanner;
    protected AtomicBoolean hasCompile = new AtomicBoolean(false);
    protected List<T> list = new ArrayList<>();

    /**
     * 把多个表达式放到库里
     *
     * @param regex
     */
    public void addRegex(String regex, T context) {
        list.add(context);
        Expression expression = new Expression(regex, EnumSet.of(ExpressionFlag.UTF8, ExpressionFlag.CASELESS, ExpressionFlag.SINGLEMATCH), list.size() - 1);
        regexs.add(expression);
        db = null;
        scanner = null;
        hasCompile.set(false);
    }

    /**
     * 完成编译
     */
    public void compile() {
        try {
            if (hasCompile.compareAndSet(false, true) && regexs.size() > 0) {
                Database db = Database.compile(regexs);
                Scanner scanner = new Scanner();
                scanner.allocScratch(db);
                this.db = db;
                this.scanner = scanner;
            }

        } catch (Exception e) {
            System.out.println("can not support this regex " + e.getMessage());
        }

    }

    /**
     * 匹配
     *
     * @param content
     * @return
     */
    public boolean match(String content) {
        if (scanner == null || db == null || hasCompile.get() == false) {
            compile();
        }
        List<Match> matches = scanner.scan(db, content);
        if (matches.size() > 0) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * 返回匹配的表达式
     *
     * @param content
     * @return
     */
    public Set<T> matchExpression(String content) {
        if (scanner == null || db == null || hasCompile.get() == false) {
            compile();
        }
        List<Match> matches = scanner.scan(db, content);
        Set<T> fireExpressions = new HashSet<>();
        if (matches.size() == 0) {
            return fireExpressions;
        }
        for (Match match : matches) {
            Integer index = match.getMatchedExpression().getId();
            fireExpressions.add(list.get(index));
        }
        return fireExpressions;
    }
}
