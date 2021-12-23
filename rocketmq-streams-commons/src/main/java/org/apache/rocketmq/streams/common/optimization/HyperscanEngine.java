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

import com.gliwka.hyperscan.wrapper.CompileErrorException;
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
import org.apache.rocketmq.streams.common.utils.StringUtil;

public class HyperscanEngine<T> implements IStreamRegex<T> {

    protected List<Expression> allHyperScanRegex = new ArrayList<>();

    protected Database db;

    protected Scanner scanner;

    protected List<T> expressionContextList = new ArrayList<>();

    protected List<Expression> notSupportCompileExpression = new ArrayList<>();

    protected List<Expression> supportCompileExpression = new ArrayList<>();

    /**
     * 把多个表达式放到库里
     *
     * @param regex
     */
    public void addRegex(String regex, T context) {
        expressionContextList.add(context);
        Expression expression = new Expression(regex, EnumSet.of(ExpressionFlag.UTF8, ExpressionFlag.CASELESS, ExpressionFlag.SINGLEMATCH), expressionContextList.size() - 1);
        allHyperScanRegex.add(expression);
        supportCompileExpression.add(expression);
        db = null;
        scanner = null;
    }

    /**
     * 完成编译
     */
    public void compile() {
        if (supportCompileExpression.isEmpty()) {
            return;
        }
        while (true) {
            Scanner scanner = null;
            try {
                if (supportCompileExpression.size() == 0) {
                    break;
                }
                Database db = Database.compile(supportCompileExpression);
                scanner = new Scanner();
                scanner.allocScratch(db);
                this.db = db;
                this.scanner = scanner;
                break;
            } catch (CompileErrorException e) {
                if (scanner != null) {
                    scanner.close();
                }
                Expression expression = e.getFailedExpression();
                this.supportCompileExpression.remove(expression);
                this.notSupportCompileExpression.add(expression);
            }
        }

    }

    /**
     * 匹配
     *
     * @param content
     * @return
     */
    public boolean match(String content) {
        if (scanner == null || db == null) {
            compile();
        }
        if (content == null || scanner == null || db == null) {
            return false;
        }
        List<Match> matches = scanner.scan(db, content);
        if (matches.size() > 0) {
            return true;
        } else {
            if (this.notSupportCompileExpression.size() > 0) {
                for (Expression expression : this.notSupportCompileExpression) {
                    String regex = expression.getExpression();
                    boolean isMatch = StringUtil.matchRegexCaseInsensitive(content, regex);
                    if (isMatch) {
                        return true;
                    }
                }
            }
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
        if (scanner == null || db == null) {
            compile();
        }
        if (content == null || scanner == null || db == null) {
            return new HashSet<>();
        }
        List<Match> matches = null;
        synchronized (this) {
            matches = scanner.scan(db, content);
        }
        Set<T> fireExpressions = new HashSet<>();
        if (this.notSupportCompileExpression.size() > 0) {
            for (Expression expression : this.notSupportCompileExpression) {
                String regex = expression.getExpression();
                boolean isMatch = StringUtil.matchRegexCaseInsensitive(content, regex);
                if (isMatch) {
                    int index = expression.getId();
                    fireExpressions.add(expressionContextList.get(index));
                }
            }
        }
        if (matches.size() > 0) {
            for (Match match : matches) {
                Integer index = match.getMatchedExpression().getId();
                fireExpressions.add(expressionContextList.get(index));
            }
        }
        return fireExpressions;
    }

    public int size() {
        return allHyperScanRegex.size();
    }
}
