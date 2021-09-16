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
package org.apache.rocketmq.streams.common.utils;

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.metadata.MetaDataField;

public class SQLUtil {
    private static final String INSERT = "INSERT INTO";
    private static final String INSERT_IGNORE = "INSERT IGNORE INTO";
    private static final String REPLACE = "REPLACE INTO";

    public static String createReplacesInsertSql(MetaData metaData, Map<String, Object> fieldName2Value,
        Boolean containsIdField) {
        String insertSQL = createInsertSql(metaData, fieldName2Value, containsIdField);
        insertSQL = insertSQL.replaceFirst(INSERT, REPLACE);
        return insertSQL;
    }

    public static String createDuplicateKeyUpdateSQL(MetaData metaData) {
        List<MetaDataField> fields = metaData.getMetaDataFields();
        StringBuilder stringBuilder = new StringBuilder();
        boolean isFirst = true;
        for (MetaDataField field : fields) {
            if (field.getFieldName().equals(metaData.getIdFieldName())) {
                continue;
            }
            if (isFirst) {
                isFirst = false;
            } else {
                stringBuilder.append(",");
            }

            String updateSQL = "`" + field.getFieldName() + "`=values(`" + field.getFieldName() + "`)";
            stringBuilder.append(updateSQL);
        }
        return stringBuilder.toString();
    }

    public static String createIgnoreInsertSql(MetaData metaData, Map<String, Object> fieldName2Value,
        Boolean containsIdField) {
        String insertSQL = createInsertSql(metaData, fieldName2Value, containsIdField);
        insertSQL = insertSQL.replaceFirst(INSERT, INSERT_IGNORE);
        return insertSQL;
    }

    public static String createInsertSql(MetaData metaData, Map<String, Object> fieldName2Value) {
        return createInsertSql(metaData, fieldName2Value, null);
    }

    public static String createInsertSql(MetaData metaData, Map<String, Object> fieldName2Value,
        Boolean containsIdField) {

        StringBuilder sql = new StringBuilder(INSERT + " " + metaData.getTableName() + "(");
        StringBuilder fieldSql = new StringBuilder();
        StringBuilder valueSql = new StringBuilder();
        if (containsIdField == null) {
            createInsertValuesSQL(metaData, fieldName2Value, fieldSql, valueSql);
        } else {
            createInsertValuesSQL(metaData, fieldName2Value, fieldSql, valueSql, containsIdField);
        }

        sql.append(fieldSql.toString());
        sql.append(")");
        sql.append(" values");
        sql.append(valueSql.toString());
        return sql.toString();
    }

    /**
     * 创建insert values部分sql，从第二个开始，带了，
     *
     * @param metaData
     * @param rows
     * @return
     */
    public static String createInsertValuesSQL(MetaData metaData, List<? extends Map<String, Object>> rows) {
        StringBuilder stringBuilder = new StringBuilder();
        for (Map<String, Object> row : rows) {
            stringBuilder.append(",");
            StringBuilder tmp = new StringBuilder();
            String value = createInsertValuesSQL(metaData, row, null, tmp);
            stringBuilder.append(value);
        }
        return stringBuilder.toString();
    }

    protected static String createInsertValuesSQL(MetaData metaData, Map<String, Object> fieldName2Value,
        StringBuilder fieldSql, StringBuilder valueSql) {
        boolean isIncrement = true;
        if (fieldName2Value.containsKey(metaData.getIdFieldName())) {
            isIncrement = false;
        }
        return createInsertValuesSQL(metaData, fieldName2Value, fieldSql, valueSql, isIncrement);
    }

    protected static String createInsertValuesSQL(MetaData metaData, Map<String, Object> fieldName2Value,
        StringBuilder fieldSql, StringBuilder valueSql, boolean containsIdField) {
        boolean isFirst = true;
        valueSql.append("(");
        //if (fieldName2Value.containsKey(metaData.getIdFieldName())) {
        //    isIncrement = false;
        //}
        Iterator<MetaDataField> it = metaData.getMetaDataFields().iterator();
        while (it.hasNext()) {
            MetaDataField field = it.next();
            if (field.getFieldName().equals(metaData.getIdFieldName())) {
                if (!containsIdField) {
                    continue;
                }
            }

            String fieldName = field.getFieldName();
            Object value = fieldName2Value.get(fieldName);
            if (value != null) {
                if (isFirst) {
                    isFirst = false;
                } else {
                    if (fieldSql != null) {
                        fieldSql.append(",");
                    }
                    valueSql.append(",");
                }
                if (fieldSql != null) {
                    fieldSql.append("`" + fieldName + "`");
                }
                valueSql.append(getFieldSqlValue(metaData, fieldName, value));
            }
        }
        valueSql.append(")");
        return valueSql.toString();
    }

    public static String getFieldSqlValue(MetaData metaData, String filedName, Object value) {
        MetaDataField field = metaData.getMetaDataField(filedName);
        if (value == null) {
            return "";
        }
        if (needQuotes(field.getDataType())) {
            // StringBuffer stringBuffer = new StringBuffer();
            // char[] chars = value.toString().toCharArray();
            // for (int i = 0; i < chars.length; i++) {
            // if (chars[i] == '\'') {
            // if (i == 0) {
            // stringBuffer.append("\\");
            // } else {
            // if (chars[i - 1] != '\\') {
            // stringBuffer.append("\\");
            // }
            // }
            // }
            // stringBuffer.append(chars[i]);
            // }
            String result = null;
            if (DataTypeUtil.isDate(field.getDataType().getDataClass())) {
                result = DateUtil.format((Date) value);
            } else if (JSONObject.class.isInstance(value)) {
                result = ((JSONObject) value).toJSONString();
            } else {
                result = value.toString();
            }
            return "'" + handleSpecialCharInSql(result) + "'";
        } else {
            if (DataTypeUtil.isBoolean(field.getDataType().getDataClass())) {
                boolean boolValue = (Boolean) value;
                return boolValue ? "1" : "0";
            }
            return value + "";
        }
    }

    /**
     * 判断插入的字段是否为字符，如果是返回true，否则返回false
     *
     * @param dataType
     * @return
     */
    public static boolean needQuotes(DataType dataType) {
        if (DataTypeUtil.isString(dataType.getDataClass()) || DataTypeUtil.isDate(dataType.getDataClass())) {
            return true;
        }
        return false;
    }

    public static String handleSpecialCharInSql(String value) {
        // \改成\\
        value = value.replaceAll("\\\\", "\\\\\\\\");
        // '改成''
        return value.replaceAll("'", "''");

    }

    /**
     * 解析sql中有默认参数的部分 例：select * from table where column_name=#{name=chris} 解析结果吗，会返回name：chris。chris会做为sql的默认值，后续可以通过脚本修改
     *
     * @param ibatisSQL
     * @return
     */
    public static JSONObject parseDefaultPara(String ibatisSQL) {
        if (StringUtil.isEmpty(ibatisSQL)) {
            return null;
        }
        List<String> vars = SQLUtil.parseIbatisSQLVars(ibatisSQL);
        if (vars == null) {
            return null;
        }
        JSONObject initParameters = new JSONObject();
        for (String var : vars) {
            int index = var.indexOf("=");
            String varName = null;
            String value = null;
            if (index != -1) {
                varName = var.substring(0, index);
                value = var.substring(index + 1, var.length() - 1);
                initParameters.put(varName, value);
                ibatisSQL = ibatisSQL.replace(var, varName);
            }
        }
        return initParameters;
    }

    /**
     * 给定ibatis builder，替换里面的变量
     *
     * @param object    对应的model
     * @param ibatisSQL 用 ibtais 格式的sql
     * @return
     */
    public static String parseIbatisSQL(Object object, String ibatisSQL) {
        boolean containsQuotation = ibatisSQL.indexOf("'#{") != -1;
        return parseIbatisSQL(object, ibatisSQL, containsQuotation);
    }

    /**
     * 给定ibatis builder，替换里面的变量
     *
     * @param object    对应的model
     * @param ibatisSQL 用 ibtais 格式的sql
     * @return
     */
    public static String parseIbatisSQL(Object object, String ibatisSQL, boolean containsQuotation) {
        if (object == null) {
            return ibatisSQL;
        }
        if (StringUtil.isEmpty(ibatisSQL)) {
            return null;
        }

        List<String> vars = parseIbatisSQLVars(ibatisSQL);
        if (vars.size() == 0) {
            return ibatisSQL;
        }
        String sql = ibatisSQL;
        for (String varName : vars) {
            Object value = getBeanFieldValue(object, varName);
            String valueSQL = null;

            if (value != null & !(value instanceof String) && !Date.class.isInstance(value)) {
                valueSQL = value.toString();
            }
            if (value instanceof String) {
                value = value.toString().replace("'", "''");
                if (value.toString().contains("\\")) {
                    value = value.toString().replaceAll("\\\\", "\\\\\\\\");
                }
                if (containsQuotation && (ibatisSQL.indexOf("'#{" + varName + "}'") > -1 || ibatisSQL.indexOf("`#{" + varName + "}`") > -1)) {
                    valueSQL = value + "";
                } else {
                    valueSQL = "'" + value + "'";
                }
            }
            if (value instanceof Date) {
                String valueDate = DateUtil.format((Date) value);
                if (containsQuotation && ibatisSQL.indexOf("'#{" + varName + "}'") > -1) {
                    valueSQL = valueDate;
                } else {
                    valueSQL = "'" + valueDate + "'";
                }

            }

            if (value == null) {
                if (containsQuotation) {
                    valueSQL = "<null>";
                } else {
                    valueSQL = "null";
                }

            }
            sql = sql.replace("#{" + varName + "}", valueSQL);
            if (containsQuotation) {
                sql = sql.replace("'<null>'", "null");
                sql = sql.replace("<null>", "null");
            }

        }
        return sql;
    }

    protected static Object getBeanFieldValue(Object object, String varName) {
        if (object instanceof JSONObject) {
            JSONObject jsonObject = (JSONObject) object;
            return jsonObject.get(varName);
        } else if (object instanceof Map) {
            Map<String, Object> paras = (Map) object;
            return paras.get(varName);
        } else {

            if (object instanceof IConfigurable && varName.equals(IConfigurable.JSON_PROPERTY)) {
                IConfigurable configurable = (IConfigurable) object;
                return configurable.toJson();
            }
            if (object instanceof BasedConfigurable && varName.equals(IConfigurable.STATUS_PROPERTY)) {
                BasedConfigurable basedConfigurable = (BasedConfigurable) object;
                return basedConfigurable.getStatus();
            }
            return ReflectUtil.getBeanFieldValue(object, varName);
        }
    }

    /**
     * 解析sql中＃{}中的字段名称
     *
     * @param ibatisSQL
     * @return
     */
    public static List<String> parseIbatisSQLVars(String ibatisSQL) {
        List<String> vars = new ArrayList<>();
        boolean startVarHeader = false;
        boolean startVar = false;
        String var = "";
        for (int i = 0; i < ibatisSQL.length(); i++) {
            String word = ibatisSQL.substring(i, i + 1);
            if ("#".equals(word)) {
                startVarHeader = true;
                continue;
            }
            if ("{".equals(word) && startVarHeader) {
                startVarHeader = false;
                startVar = true;
                continue;
            }
            if ("}".equals(word)) {
                startVar = false;
                vars.add(var);
                var = "";
                continue;
            }
            if (startVar) {
                var += word;
            }
        }
        return vars;
    }

    public static String createInSql(Collection<String> collection) {
        if (collection == null) {
            return "";
        }
        String[] values = new String[collection.size()];
        Iterator<String> it = collection.iterator();
        int i = 0;
        while (it.hasNext()) {
            String value = it.next();
            values[i] = value;
            i++;
        }
        return createInSql(values);
    }

    public static String createInSql(String... values) {
        return createInSql(true, values);
    }

    public static String createInSql(boolean isString, String... values) {
        if (values == null || values.length == 0) {
            return "";
        }
        StringBuilder stringBuilder = new StringBuilder();
        boolean isFirst = true;
        for (String value : values) {
            if (isFirst) {
                isFirst = false;
            } else {
                if (StringUtil.isNotEmpty(value)) {
                    stringBuilder.append(",");
                }
            }
            if (StringUtil.isNotEmpty(value)) {
                if (!isString) {
                    stringBuilder.append(value);
                } else {
                    StringBuilder append = stringBuilder.append("'" + value + "'");
                }

            }
        }
        return stringBuilder.toString();
    }

}
