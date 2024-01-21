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

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.streams.common.cache.ByteArrayMemoryTable;
import org.apache.rocketmq.streams.common.cache.compress.AbstractMemoryTable;
import org.apache.rocketmq.streams.common.cache.softreference.ICache;
import org.apache.rocketmq.streams.common.cache.softreference.impl.SoftReferenceCache;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.interfaces.IDim;
import org.apache.rocketmq.streams.common.threadpool.ScheduleFactory;
import org.apache.rocketmq.streams.common.utils.DataTypeUtil;
import org.apache.rocketmq.streams.common.utils.IdUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.dim.index.DimIndex;
import org.apache.rocketmq.streams.dim.index.IndexExecutor;
import org.apache.rocketmq.streams.filter.builder.ExpressionBuilder;
import org.apache.rocketmq.streams.filter.function.expression.Equals;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.filter.operator.expression.RelationExpression;
import org.apache.rocketmq.streams.script.ScriptComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 这个结构代表一张表 存放表的全部数据和索引
 */
public abstract class AbstractDim extends BasedConfigurable implements IDim {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDim.class);
    /**
     * 软引用缓存，最大可能保存索引执行器，避免频繁创建，带来额外开销 同时会保护内存不被写爆，当内存不足时自动回收内存
     */
    private static final ICache<String, IndexExecutor> cache = new SoftReferenceCache<>();
    /**
     * 同步数据的事件间隔，单位是秒
     */
    protected Long pollingTimeSeconds = 60L;
    /**
     * 支持多组索引，如果一个索引是组合索引，需要拼接成一个string，用;分割 建立索引后，会创建索引的数据结构，类似Map<String,List<RowId>，可以快速定位，无索引会全表扫描，不建议使用 如有两组索引：1.name 2. ip;address
     */
    protected List<String> indexs = new ArrayList<>();
    protected boolean isLarge = false;//if isLarge=true use MapperByteBufferTable 内存结构
    protected String filePath;
    /**
     * 把表数据转化成二进制存储在CompressTable中
     */
    protected transient volatile AbstractMemoryTable dataCache;
    /**
     * 建立名单的时候，可以指定多组索引，索引的值当作key，row在datacache的index当作value，可以快速匹配索引对应的row key：索引的值 value：row在dataCache的index当作value，可以快速匹配索引对应的row
     */
    protected transient DimIndex nameListIndex;
    protected transient Set<String> columnNames;

    //protected String index;//只是做标记，为了是简化indexs的赋值

    public AbstractDim() {
        this.setType(IDim.TYPE);
    }

    /**
     * 全表扫描，做表达式匹配，返回全部匹配结果。join中有使用
     *
     * @param expressionStr expression string
     * @param msg           msg
     * @param needAll       need all
     * @return list map
     */
    public static List<Map<String, Object>> matchExpressionByLoop(Iterator<Map<String, Object>> it, String expressionStr, JSONObject msg, boolean needAll) {
        return matchExpressionByLoop(it, expressionStr, msg, needAll, null, new HashSet<>());
    }

    /**
     * 全表扫描，做表达式匹配，返回全部匹配结果。join中有使用
     *
     * @param expressionStr expression string
     * @param msg           msg
     * @param needAll       need all
     * @return list map
     */
    public static List<Map<String, Object>> matchExpressionByLoop(Iterator<Map<String, Object>> it, String expressionStr, JSONObject msg, boolean needAll, String script, Set<String> colunmNames) {
        List<Map<String, Object>> rows = new ArrayList<>();
        Rule ruleTemplete = ExpressionBuilder.createRule("tmp", "tmpRule", expressionStr);
        while (it.hasNext()) {
            Map<String, Object> oldRow = it.next();
            Map<String, Object> newRow = isMatch(ruleTemplete, oldRow, msg, script, colunmNames);
            if (newRow != null) {
                rows.add(newRow);
                if (!needAll) {
                    return rows;
                }
            }
        }
        return rows;
    }

    /**
     * 和维表的一行数据进行匹配，如果维表中有函数，先执行函数
     *
     * @param ruleTemplate rule template
     * @param dimRow       dim row
     * @param msgRow       msg row
     * @param script       script
     * @return map
     */
    public static Map<String, Object> isMatch(Rule ruleTemplate, Map<String, Object> dimRow, JSONObject msgRow, String script, Set<String> columnNames) {
        Map<String, Object> oldRow = dimRow;
        Map<String, Object> newRow = executeScript(oldRow, script);
        if (ruleTemplate == null) {
            return newRow;
        }
        Rule rule = ruleTemplate.copy();
        Map<String, Expression> expressionMap = new HashMap<>();
        String dimAsName = null;
        ;
        for (Expression expression : rule.getExpressionMap().values()) {
            expressionMap.put(expression.getName(), expression);
            if (expression instanceof RelationExpression) {
                continue;
            }
            Object object = expression.getValue();
            if (object != null && DataTypeUtil.isString(object.getClass())) {
                String fieldName = (String) object;
                Object value = newRow.get(fieldName);
                if (value != null) {
                    Expression e = expression.copy();
                    e.setValue(value.toString());
                    expressionMap.put(e.getName(), e);
                }
            }
            if (expression.getVarName().contains(".")) {
                String[] values = expression.getVarName().split("\\.");
                if (values.length == 2) {
                    String asName = values[0];
                    String varName = values[1];
                    if (columnNames.contains(varName)) {
                        dimAsName = asName;
                    }
                }

            }
        }
        rule.setExpressionMap(expressionMap);
        rule.initElements();
        JSONObject copyMsg = msgRow;
        if (StringUtil.isNotEmpty(dimAsName)) {
            copyMsg = new JSONObject(msgRow);
            for (String key : newRow.keySet()) {
                copyMsg.put(dimAsName + "." + key, newRow.get(key));
            }
        }
        boolean matched = rule.execute(copyMsg);
        if (matched) {
            return newRow;
        }
        return null;
    }

    protected static Map<String, Object> executeScript(Map<String, Object> oldRow, String script) {
        if (script == null) {
            return oldRow;
        }
        ScriptComponent scriptComponent = ScriptComponent.getInstance();
        JSONObject msg = new JSONObject();
        msg.putAll(oldRow);
        scriptComponent.getService().executeScript(msg, script);
        return msg;
    }

    @Override public String addIndex(String... fieldNames) {
        return addIndex(this.indexs, fieldNames);
    }

    @Override protected boolean initConfigurable() {
        return super.initConfigurable();
    }

    @Override public void startLoadDimData() {
        loadNameList();
        ScheduleFactory.getInstance().execute(getNameSpace() + "-" + getName() + "-dim_schedule", this::loadNameList, pollingTimeSeconds, pollingTimeSeconds, TimeUnit.SECONDS);
    }

    /**
     * 加载维表数据 创建索引
     */
    protected void loadNameList() {
        try {
            LOGGER.info("[{}][{}] Dim_Begin_Polling_Data", IdUtil.instanceId(), getName());
            //全表数据
            AbstractMemoryTable dataCacheVar = loadData();
            this.dataCache = dataCacheVar;
            this.nameListIndex = buildIndex(dataCacheVar);
            this.columnNames = this.dataCache.getCloumnName2Index().keySet();
        } catch (Exception e) {
            LOGGER.error("[{}][{}] Dim_Load_Configurables_Error", IdUtil.instanceId(), getName(), e);
        }
    }

    /**
     * 给维表生成索引数据结构
     *
     * @param dataCacheVar 维表
     * @return dimIndex
     */
    protected DimIndex buildIndex(AbstractMemoryTable dataCacheVar) {
        DimIndex dimIndex = new DimIndex(this.indexs);
        dimIndex.buildIndex(dataCacheVar);
        return dimIndex;
    }

    @Override
    public List<Map<String, Object>> matchExpression(String msgFieldName, String dimFieldName, JSONObject msg) {
        return matchExpression("(" + msgFieldName + "," + dimFieldName + ")", msg, true, null);
    }

    /**
     * 先找索引，如果有索引，通过索引匹配。如果没有，全表扫表.
     *
     * @param expressionStr 表达式
     * @param msg           消息
     * @return 只返回匹配的第一行
     */
    @Override public Map<String, Object> matchExpression(String expressionStr, JSONObject msg) {
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
    @Override public List<Map<String, Object>> matchExpression(String expressionStr, JSONObject msg, boolean needAll, String script) {
        IndexExecutor indexNamelistExecutor = getOrCreateIndexExecutor(expressionStr);
        if (indexNamelistExecutor.isSupportIndex()) {
            return matchExpressionByIndex(indexNamelistExecutor, msg, needAll, script);
        } else {
            return matchExpressionByLoop(dataCache.rowIterator(), expressionStr, msg, needAll, script, columnNames);
        }
    }

    /**
     * 全表扫描，做表达式匹配，返回全部匹配结果
     *
     * @param expressionStr expression string
     * @param msg           msg
     * @param needAll       need all
     * @return list map
     */
    protected List<Map<String, Object>> matchExpressionByLoop(String expressionStr, JSONObject msg, boolean needAll) {
        AbstractMemoryTable dataCache = this.dataCache;
        return matchExpressionByLoop(dataCache.rowIterator(), expressionStr, msg, needAll, null, columnNames);
    }

    /**
     * 根据join条件设置索引
     */
    public void createIndexByJoinCondition(String expressionStr, IDimField dimField) {
        List<Expression> expressions = new ArrayList<>();
        List<RelationExpression> relationExpressions = new ArrayList<>();
        Expression expression = ExpressionBuilder.createOptimizationExpression("tmp", "tmp", expressionStr, expressions, relationExpressions);

        RelationExpression relationExpression = null;
        if (expression instanceof RelationExpression) {
            relationExpression = (RelationExpression) expression;
            if (!"and".equals(relationExpression.getRelation())) {
                return;
            }
        }

        List<Expression> indexExpressions = new ArrayList<>();
        List<Expression> otherExpressions = new ArrayList<>();
        if (relationExpression != null) {
            Map<String, Expression> map = new HashMap<>();
            for (Expression tmp : expressions) {
                map.put(tmp.getName(), tmp);
            }
            for (Expression tmp : relationExpressions) {
                map.put(tmp.getName(), tmp);
            }
            List<String> expressionNames = relationExpression.getValue();
            relationExpression.setValue(new ArrayList<>());
            for (String expressionName : expressionNames) {
                Expression subExpression = map.get(expressionName);
                if (subExpression != null && !(subExpression instanceof RelationExpression) && dimField.isDimField(subExpression.getValue())) {
                    indexExpressions.add(subExpression);
                } else {
                    otherExpressions.add(subExpression);
                    relationExpression.getValue().add(subExpression.getName());
                }
            }

        } else {
            indexExpressions.add(expression);
        }

        List<String> fieldNames = new ArrayList<>();

        for (Expression expre : indexExpressions) {
            if (expre instanceof RelationExpression) {
                continue;
            }
            String indexName = expre.getValue().toString();
            if (Equals.isEqualFunction(expre.getFunctionName()) && dimField.isDimField(expre.getValue())) {

                fieldNames.add(indexName);

            }
        }

        String[] indexFieldNameArray = new String[fieldNames.size()];
        int i = 0;
        for (String fieldName : fieldNames) {
            indexFieldNameArray[i] = fieldName;
            i++;
        }
        Arrays.sort(indexFieldNameArray);
        String index = MapKeyUtil.createKey(indexFieldNameArray);
        if (this.getIndexs().contains(index)) {
            return;
        }
        if (indexFieldNameArray.length > 0) {
            this.addIndex(indexFieldNameArray);
        }
    }

    protected AbstractMemoryTable loadData() {
        AbstractMemoryTable memoryTable = null;
//        if (!isLarge) {
        memoryTable = new ByteArrayMemoryTable();
        loadData2Memory(memoryTable);
        LOGGER.info("[{}][{}] Init_ByteArrayMemoryTable", IdUtil.instanceId(), getName());
//        } else {
//            Date date = new Date();
//            try {
//                memoryTable = MappedByteBufferTable.Creator.newCreator(filePath, date, pollingTimeSeconds.intValue()).create(table -> loadData2Memory(table));
//            } catch (IOException e) {
//                LOGGER.error("[{}][{}] Init_MappedByteBufferTable_Error", IdUtil.instanceId(), getConfigureName(), e);
//            }
//            LOGGER.info("[{}][{}] Init_MappedByteBufferTable", IdUtil.instanceId(), getConfigureName());
//        }
        return memoryTable;
    }

    protected abstract void loadData2Memory(AbstractMemoryTable table);

    @Override public void destroy() {
        super.destroy();
        ScheduleFactory.getInstance().cancel(getNameSpace() + "-" + getName() + "-dim_schedule");
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

    protected List<Map<String, Object>> matchExpressionByIndex(IndexExecutor executor, JSONObject msg, boolean needAll, String script) {
        return executor.match(msg, this, needAll, script);
    }

    /**
     * 每个表达式一个执行器，主要是执行非索引的过滤能力
     *
     * @param expressionStr
     * @return
     */
    protected IndexExecutor getOrCreateIndexExecutor(String expressionStr) {
        IndexExecutor indexNamelistExecutor = cache.get(expressionStr);
        if (indexNamelistExecutor == null) {
            indexNamelistExecutor = new IndexExecutor(expressionStr, getNameSpace(), this.indexs, dataCache.getCloumnName2DatatType().keySet());
            cache.put(expressionStr, indexNamelistExecutor);
        }
        return indexNamelistExecutor;
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

    public Long getPollingTimeSeconds() {
        return pollingTimeSeconds;
    }

    public void setPollingTimeSeconds(Long pollingTimeSeconds) {
        this.pollingTimeSeconds = pollingTimeSeconds;
    }

    @Override public List<String> getIndexs() {
        return indexs;
    }

    public void setIndexs(List<String> indexs) {
        this.indexs = indexs;
    }

    public AbstractMemoryTable getDataCache() {
        return dataCache;
    }

    public boolean isLarge() {
        return isLarge;
    }

    public void setLarge(boolean large) {
        isLarge = large;
    }

    public DimIndex getNameListIndex() {
        return nameListIndex;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public static interface IDimField {
        boolean isDimField(Object fieldName);
    }
}
