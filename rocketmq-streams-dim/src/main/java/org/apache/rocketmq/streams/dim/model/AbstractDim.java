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
package org.apache.rocketmq.streams.dim.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.alibaba.fastjson.JSONObject;

import org.apache.rocketmq.streams.dim.index.IndexExecutor;
import org.apache.rocketmq.streams.dim.index.DimIndex;
import org.apache.rocketmq.streams.filter.builder.ExpressionBuilder;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.filter.operator.expression.RelationExpression;
import org.apache.rocketmq.streams.common.cache.softreference.ICache;
import org.apache.rocketmq.streams.common.cache.softreference.impl.SoftReferenceCache;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.cache.CompressTable;
import org.apache.rocketmq.streams.common.utils.DataTypeUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 这个结构代表一张表 存放表的全部数据和索引
 */
public abstract class AbstractDim extends BasedConfigurable {

    private static final Log LOG = LogFactory.getLog(AbstractDim.class);

    public static final String TYPE = "nameList";

    /**
     * 同步数据的事件间隔，单位是分钟
     */
    protected Long pollingTimeMintue = 60L;

    /**
     * 支持多组索引，如果一个索引是组合索引，需要拼接成一个string，用;分割 建立索引后，会创建索引的数据结构，类似Map<String,List<RowId>，可以快速定位，无索引会全表扫描，不建议使用 如有两组索引：1.name 2. ip;address
     */
    protected List<String> indexs = new ArrayList<>();

    /**
     * 把表数据转化成二进制存储在CompressTable中
     */
    protected transient volatile CompressTable dataCache;

    /**
     * 建立名单的时候，可以指定多组索引，索引的值当作key，row在datacache的index当作value，可以快速匹配索引对应的row key：索引的值 value：row在dataCache的index当作value，可以快速匹配索引对应的row
     */
    protected transient DimIndex nameListIndex;

    //是否是唯一索引，唯一索引会用IntValueKV存储，有更高的压缩率
    protected boolean isUniqueIndex = false;

    //定时加载表数据到内存
    protected transient ScheduledExecutorService executorService;

    public AbstractDim() {
        this.setType(TYPE);
    }

    //protected String index;//只是做标记，为了是简化indexs的赋值

    public String addIndex(String... fieldNames) {
        return addIndex(this.indexs, fieldNames);
    }

    @Override
    protected boolean initConfigurable() {
        boolean success = super.initConfigurable();
        loadNameList();
        executorService = new ScheduledThreadPoolExecutor(3);
        executorService.scheduleWithFixedDelay(new Runnable() {

            @Override
            public void run() {
                loadNameList();
            }
        }, pollingTimeMintue, pollingTimeMintue, TimeUnit.MINUTES);
        return success;
    }

    /**
     * 加载维表数据 创建索引
     */
    protected void loadNameList() {
        try {
            LOG.info(getConfigureName() + " begin polling data");
            //全表数据
            CompressTable dataCacheVar = loadData();
            this.dataCache = dataCacheVar;
            this.nameListIndex = buildIndex(dataCacheVar);
        } catch (Exception e) {
            LOG.error("Load configurables error:" + e.getMessage(), e);
        }
    }

    /**
     * 给维表生成索引数据结构
     *
     * @param dataCacheVar 维表
     * @return
     */
    protected DimIndex buildIndex(CompressTable dataCacheVar) {
        DimIndex dimIndex = new DimIndex(this.indexs);
        dimIndex.setUnique(isUniqueIndex);
        dimIndex.buildIndex(dataCacheVar);
        return dimIndex;
    }

    /**
     * 根据索引名和索引值查询匹配的行号
     *
     * @param indexName
     * @param indexValue
     * @return
     */
    public List<Integer> findRowIdByIndex(String indexName, String indexValue) {
        return nameListIndex == null ? Collections.emptyList() : nameListIndex.getRowIds(indexName, indexValue);
    }

    /**
     * 软引用缓存，最大可能保存索引执行器，避免频繁创建，带来额外开销 同时会保护内存不被写爆，当内存不足时自动回收内存
     */
    private static ICache<String, IndexExecutor> cache = new SoftReferenceCache<>();

    /**
     * 先找索引，如果有索引，通过索引匹配。如果没有，全表扫表.
     *
     * @param expressionStr 表达式
     * @param msg           消息
     * @return 只返回匹配的第一行
     */
    public Map<String, Object> matchExpression(String expressionStr, JSONObject msg) {
        List<Map<String, Object>> rows = matchExpression(expressionStr, msg, true, null);
        if (rows != null && rows.size() > 0) {
            return rows.get(0);
        }
        return null;
    }

    /**
     * 先找索引，如果有索引，通过索引匹配。如果没有，全表扫表
     *
     * @param expressionStr 表达式
     * @param msg           消息
     * @return 返回全部匹配的行
     */
    public List<Map<String, Object>> matchExpression(String expressionStr, JSONObject msg, boolean needAll, String script) {
        IndexExecutor indexNamelistExecutor = cache.get(expressionStr);
        if (indexNamelistExecutor == null) {
            indexNamelistExecutor = new IndexExecutor(expressionStr, getNameSpace(), this.indexs);
            cache.put(expressionStr, indexNamelistExecutor);
        }
        if (indexNamelistExecutor.isSupport()) {
            return indexNamelistExecutor.match(msg, this, needAll, script);
        } else {
            return matchExpressionByLoop(expressionStr, msg, needAll);
        }
    }

    /**
     * 全表扫描，做表达式匹配，返回全部匹配结果
     *
     * @param expressionStr
     * @param msg
     * @param needAll
     * @return
     */
    protected List<Map<String, Object>> matchExpressionByLoop(String expressionStr, JSONObject msg, boolean needAll) {
        CompressTable dataCache = this.dataCache;
        List<Map<String, Object>> rows = matchExpressionByLoop(dataCache.newIterator(), expressionStr, msg, needAll);
        return rows;
    }

    /**
     * 全表扫描，做表达式匹配，返回全部匹配结果。join中有使用
     *
     * @param expressionStr
     * @param msg
     * @param needAll
     * @return
     */
    public static List<Map<String, Object>> matchExpressionByLoop(Iterator<Map<String, Object>> it, String expressionStr, JSONObject msg, boolean needAll) {
        List<Map<String, Object>> rows = new ArrayList<>();
        while (it.hasNext()) {
            Map<String, Object> values = it.next();
            Rule ruleTemplete = ExpressionBuilder.createRule("tmp", "tmpRule", expressionStr);
            Rule rule = ruleTemplete.copy();
            Map<String, Expression> expressionMap = new HashMap<>();
            for (Expression expression : rule.getExpressionMap().values()) {
                expressionMap.put(expression.getConfigureName(), expression);
                if (RelationExpression.class.isInstance(expression)) {
                    continue;
                }
                Object object = expression.getValue();
                if (object != null && DataTypeUtil.isString(object.getClass())) {
                    String fieldName = (String)object;
                    Object value = values.get(fieldName);
                    if (value != null) {
                        Expression e = expression.copy();
                        e.setValue(value.toString());
                        expressionMap.put(e.getConfigureName(), e);
                    }
                }
            }
            rule.setExpressionMap(expressionMap);
            boolean matched = rule.execute(msg);
            if (matched) {
                rows.add(values);
                if (needAll == false) {
                    return rows;
                }
            }
        }
        return rows;
    }

    protected abstract CompressTable loadData();

    @Override
    public void destroy() {
        super.destroy();
        executorService.shutdown();
    }

    /**
     * 设置索引
     *
     * @param indexs 字段名称，多个字段";"分隔
     */
    public void setIndex(String indexs) {
        if (StringUtil.isEmpty(indexs)) {
            return;
        }
        List<String> tmp = new ArrayList<>();
        String[] values = indexs.split(";");
        this.addIndex(tmp, values);
        this.indexs = tmp;
    }

    /**
     * 建议指定索引，会基于索引建立map，对于等值的判断，可以快速匹配
     *
     * @param fieldNames
     */
    private String addIndex(List<String> indexs, String... fieldNames) {
        if (fieldNames == null) {
            return null;
        }
        Arrays.sort(fieldNames);
        String index = MapKeyUtil.createKey(fieldNames);
        if (StringUtil.isNotEmpty(index)) {
            indexs.add(index);
        }
        return index;
    }

    public Long getPollingTimeMintue() {
        return pollingTimeMintue;
    }

    public void setPollingTimeMintue(Long pollingTimeMintue) {
        this.pollingTimeMintue = pollingTimeMintue;
    }

    public List<String> getIndexs() {
        return indexs;
    }

    public void setIndexs(List<String> indexs) {
        this.indexs = indexs;
    }

    public CompressTable getDataCache() {
        return dataCache;
    }

    public boolean isUniqueIndex() {
        return isUniqueIndex;
    }

    public void setUniqueIndex(boolean uniqueIndex) {
        isUniqueIndex = uniqueIndex;
    }
}
