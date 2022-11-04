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
package org.apache.rocketmq.streams.storage.orc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sun.corba.se.spi.ior.ObjectKey;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.datatype.DateDataType;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.storage.utils.TermMatchUtil;

public class RowIterator implements Iterator<List<JSONObject>> {
    protected String[] columnNames;
    protected transient RecordReader rows;
    protected transient VectorizedRowBatch batch;

    /**
     * 谓词下推,词搜索
     */
    protected  List<Integer> batchIndexs;//指定扫描的batch
    protected  Object[] terms;
    protected  boolean supportPrefix;
    protected  String queryColumn;
    protected  DataType queryColumnDataType;
    protected transient Integer currentIndex=0;//当前执行到哪个batch

    /**
     * 谓词下推,范围查找
     */
    protected Object min;
    protected Object max;

    public RowIterator(String orcFilePath,TypeDescription schema, String... columnNames){
        this.columnNames=columnNames;
        try {
            Configuration conf = new Configuration();
            Reader reader = OrcFile.createReader(new Path(orcFilePath),
                OrcFile.readerOptions(conf));
            Reader.Options readerOptions = new Reader.Options(conf);
            this.rows = reader.rows(readerOptions.schema(schema));
            this.batch = schema.createRowBatch();
        }catch (Exception e){
            e.printStackTrace();
            throw new RuntimeException("create orc read error ",e);
        }

    }

    @Override public boolean hasNext() {
        try {
            if(batchIndexs==null){
                return rows.nextBatch(batch);
            }else {
                if(currentIndex>=batchIndexs.size()){
                    return false;
                }
                Integer batchIndex=batchIndexs.get(currentIndex);
                rows.seekToRow(batchIndex);
                currentIndex++;
                return rows.nextBatch(batch);
            }

        }catch (Exception e){
            throw new RuntimeException("rows nextbatch error ",e);
        }

    }

    @Override public List<JSONObject> next() {

        ColumnVector[] vectors=new ColumnVector[columnNames.length];
        for(int i=0;i<vectors.length;i++){
            vectors[i]= batch.cols[i];
        }
        try {
            List<JSONObject> rows=new ArrayList<>();
            for(int rowIndex=0; rowIndex < batch.size; rowIndex++) {
                JSONObject row=new JSONObject();
                for(int i=0;i<vectors.length;i++){
                    ColumnVector vector=vectors[i];
                    Object value=getColumnValue(vector,rowIndex);
                    row.put(columnNames[i],value);
                }
                boolean isMatch=matchTerm(row);
                if(isMatch){
                    rows.add(row);
                }

            }
            return rows;
        }catch (Exception e){
            throw new RuntimeException("GET BATCH DATA FROM ORC ERROR ",e);
        }


    }

    /**
     * 当前列是否包含
     * @param row
     * @return
     */
    protected boolean matchTerm(JSONObject row) {
        if(StringUtil.isEmpty(queryColumn)||queryColumnDataType==null){
            return true;
        }

        Object columnValue=row.get(queryColumn);
        if(columnValue==null){
            return false;
        }
        if(terms!=null){
            for(Object term:terms){
                if(term==null){
                    continue;
                }
                    boolean isMatch= TermMatchUtil.matchTerm(columnValue,queryColumnDataType,term,isSupportPrefix());
                if(isMatch){
                    return isMatch;
                }
            }
        }else if(min!=null||max!=null){
            boolean isMatch= TermMatchUtil.matchBetween(columnValue,queryColumnDataType,min,max);
            if(isMatch){
                return isMatch;
            }
        }

        return false;
    }

    protected Object getColumnValue(ColumnVector vector, int rowIndex) {

        try {
            if(BytesColumnVector.class.isInstance(vector)){
                BytesColumnVector bytesColumnVector=(BytesColumnVector)vector;
                return bytesColumnVector.toString(rowIndex);
            }else if(LongColumnVector.class.isInstance(vector)){
                LongColumnVector columnVector=(LongColumnVector)vector;
                return columnVector.vector[rowIndex];
            }else {
                throw new RuntimeException("can not support this columnVector "+vector.toString());
            }

            //todo other columnVector impl
        }catch (Exception e){
            throw new RuntimeException("set value 2 orc error",e);
        }
    }


    public List<Integer> getBatchIndexs() {
        return batchIndexs;
    }

    public void setBatchIndexs(List<Integer> batchIndexs) {
        this.batchIndexs = batchIndexs;
    }



    public boolean isSupportPrefix() {
        return supportPrefix;
    }

    public void setSupportPrefix(boolean supportPrefix) {
        this.supportPrefix = supportPrefix;
    }

    public String getQueryColumn() {
        return queryColumn;
    }

    public void setQueryColumn(String queryColumn) {
        this.queryColumn = queryColumn;
    }

    public void setTerms(Object[] terms) {
        this.terms = terms;
    }

    public DataType getQueryColumnDataType() {
        return queryColumnDataType;
    }

    public void setQueryColumnDataType(DataType queryColumnDataType) {
        this.queryColumnDataType = queryColumnDataType;
    }

    public Object getMin() {
        return min;
    }

    public void setMin(Object min) {
        this.min = min;
    }

    public Object getMax() {
        return max;
    }

    public void setMax(Object max) {
        this.max = max;
    }
}
