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

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.streams.common.configuration.ConfigurationKey;
import org.apache.rocketmq.streams.common.configuration.SystemContext;

/**
 * regex engine
 */
public class RegexEngine<T> {

    protected static final String RE2J_ENGINE = "re2j";
    protected static final String HYPER_SCAN_ENGINE = "hyperscan";
    protected AtomicBoolean hasCompile = new AtomicBoolean(false);
    protected IStreamRegex engine = new HyperscanEngine<>();

    public RegexEngine() {
        String option = SystemContext.getStringParameter(ConfigurationKey.DIPPER_REGEX_ENGINE);
        if (RE2J_ENGINE.equalsIgnoreCase(option)) {
            engine = new Re2Engine<>();
        }
    }

    public RegexEngine(boolean isRE2J) {
        if (isRE2J) {
            engine = new Re2Engine<>();
        }
    }

    public void addRegex(String regex, T context) {
        engine.addRegex(regex, context);
        hasCompile.set(false);
    }

    public void compile() {
        if (!hasCompile.compareAndSet(false, true)) {
            return;
        }
        engine.compile();
    }

    public boolean match(String content) {
        if (!hasCompile.get()) {
            compile();
        }
        return engine.match(content);
    }

    public Set<T> matchExpression(String content) {
        if (!hasCompile.get()) {
            compile();
        }
        return engine.matchExpression(content);
    }

    public int size() {
        return engine.size();
    }
}
