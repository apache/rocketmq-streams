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

import com.alibaba.fastjson.JSONObject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.Reader;
import org.apache.orc.StripeStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.impl.ColumnStatisticsImpl;
import org.apache.rocketmq.streams.common.datatype.BooleanDataType;
import org.apache.rocketmq.streams.common.datatype.ByteDataType;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.datatype.DateDataType;
import org.apache.rocketmq.streams.common.datatype.DoubleDataType;
import org.apache.rocketmq.streams.common.datatype.FloatDataType;
import org.apache.rocketmq.streams.common.datatype.IntDataType;
import org.apache.rocketmq.streams.common.datatype.ListDataType;
import org.apache.rocketmq.streams.common.datatype.LongDataType;
import org.apache.rocketmq.streams.common.datatype.MapDataType;
import org.apache.rocketmq.streams.common.datatype.ShortDataType;
import org.apache.rocketmq.streams.common.datatype.StringDataType;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.metadata.MetaDataField;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.storage.utils.TermMatchUtil;

public class ORCFile {
    protected transient Writer writer ;
    protected transient TypeDescription schema;
    protected transient String orcFilePath;
    protected VectorizedRowBatch batch ;
    protected MetaData metaData;

    protected Map<String,Integer> columnName2Index=new HashMap<>();//列名和列在metadata中的index

    public ORCFile(String orcFilePath,MetaData metaData){
        this.orcFilePath=orcFilePath;
        this.metaData=metaData;

        TypeDescription schema = TypeDescription.createStruct();
        List<MetaDataField> fields = metaData.getMetaDataFields();
        int i=0;
        for(MetaDataField field:fields){
            String fieldName=field.getFieldName();
            schema.addField(fieldName, createType(field.getDataType()));
            columnName2Index.put(fieldName,i);
            i++;
        }
        this.schema=schema;
    }

    public void insertData(List<JSONObject> msgs) {
        if(msgs==null){
            return ;
        }
        if(this.writer==null||this.batch==null){
            synchronized (this){
                if(this.writer==null||this.batch==null){
                    try {
                        Configuration conf = new Configuration();
                        this.writer = OrcFile.createWriter(new Path(orcFilePath),
                            OrcFile.writerOptions(conf)
                                .rowIndexStride(10000)
                                .setSchema(schema));
                        this.batch = schema.createRowBatch();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }

        }



        try {
            for(int i=0; i < msgs.size(); i++) {
                JSONObject jsonObject=msgs.get(i);
                int rowIndex = batch.size++;
                for(int j=0;j<metaData.getMetaDataFields().size();j++) {
                    ColumnVector vector =  batch.cols[j];
                    MetaDataField metaDataField = (MetaDataField)metaData.getMetaDataFields().get(j);
                    setColumnValue(vector,jsonObject.get(metaDataField.getFieldName()),rowIndex);


                }
                if (batch.size == batch.getMaxSize()) {
                    writer.addRowBatch(batch);
                    batch.reset();
                }
            }
            if (batch.size != 0) {
                writer.addRowBatch(batch);
                batch.reset();
            }
        }catch (Exception e){
            throw new RuntimeException("insert data 2 orc error ",e);
        }
    }


    public void flush(){
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public RowIterator queryRows(String... columnNames) {
        TypeDescription schema = TypeDescription.createStruct();
        for(String columnName:columnNames){
            int index=columnName2Index.get(columnName);
            MetaDataField field=(MetaDataField)metaData.getMetaDataFields().get(index);
            String fieldName=field.getFieldName();
            schema.addField(fieldName, createType(field.getDataType()));
        }
      return new RowIterator(orcFilePath,schema,columnNames);
    }

    public RowIterator queryRowsByTerms(String queryColumn,String viewColumn, boolean supportPrefix, Object... terms){
        return queryRows(queryColumn,viewColumn,supportPrefix,false,terms);
    }
    public RowIterator queryRowsByBetween(String queryColumn,String viewColumn,Object min,Object max){
        return queryRows(queryColumn,viewColumn,false,true,new Object[]{min,max});
    }


    /**
     * 统计orc文件行条数
     */
    public long count() {
        Reader reader = getORCReader();
        OrcProto.FileTail fileTail=reader.getFileTail();
        return fileTail.getFooter().getNumberOfRows();
    }

    /**
     * 关闭orc文件
     */
    public void close(){
        try {
            writer.close();
        } catch (IOException e) {
            throw new RuntimeException("close orc file error "+orcFilePath);
        }
    }

    /**
     * 返回包含term的行
     * 获取queryColumn，queryColumn列的值，并根据下推的谓词完成过滤
     * @param queryColumn 需要查询的cloumn
     * @param viewColumn 额外需要展示的列
     * @param supportPrefix 谓词匹配是否考虑前缀匹配，只针对字符串生效
     * @param terms 待搜索的词
     * @return
     */
    protected RowIterator queryRows(String queryColumn,String viewColumn, boolean supportPrefix,boolean isBetween, Object... terms){
        String[] columnNames=new String[]{queryColumn,viewColumn};
        Reader reader=getORCReader();
        List<OrcProto.ColumnStatistics> columnStatisticsList=reader.getOrcProtoFileStatistics();
        Integer columnIndex=columnName2Index.get(queryColumn);
        if(columnIndex==null){
            throw new RuntimeException(queryColumn+" column is not exist in "+this.orcFilePath+" orc file ");
        }

        /**
         * 判断搜索词是否在当前文件
         */
        OrcProto.ColumnStatistics columnStatistics=columnStatisticsList.get(columnIndex+1);
        MetaDataField metaDataField=(MetaDataField) metaData.getMetaDataFields().get(columnIndex);
        Object min=getMinColumnValue(metaDataField.getDataType(),columnStatistics);
        Object max=getMaxColumnValue(metaDataField.getDataType(),columnStatistics);
        if(!isMatch(metaDataField.getDataType(),min,max,terms)){
            return null;
        }

        /**
         * 找出包含搜索词的Stripe
         */

        try {
            List<Integer> stripeIndexs=new ArrayList<>();
            List<OrcProto.StripeStatistics> list = reader.getOrcProtoStripeStatistics();
            List<StripeStatistics> stripeStatistics= reader.getStripeStatistics();
            int stripeIndex=0;
            for(StripeStatistics statistics:stripeStatistics){
                ColumnStatistics statistic=statistics.getColumnStatistics()[columnIndex+1];
                ColumnStatisticsImpl tripeColumnStatistics=(ColumnStatisticsImpl)statistic;
                Object minValue=ReflectUtil.getDeclaredField(tripeColumnStatistics,"minimum");
                Object maxValue=ReflectUtil.getDeclaredField(tripeColumnStatistics,"maximum");
                if(isMatch(metaDataField.getDataType(),minValue,maxValue,terms)){
                    stripeIndexs.add(stripeIndex);
                }
                stripeIndex++;
            }
            if(stripeIndexs.size()==0){
                return null;
            }
            RowIterator rowIterator= new RowIterator(orcFilePath,schema,columnNames);
        //    rowIterator.setBatchIndexs(stripeIndexs);
            rowIterator.setQueryColumn(queryColumn);
            rowIterator.setQueryColumnDataType(metaDataField.getDataType());
            rowIterator.setSupportPrefix(supportPrefix);
            if(isBetween){
                rowIterator.setMin(terms[0]);
                rowIterator.setMax(terms[1]);
            }else {
                rowIterator.setTerms(terms)    ;
            }

            return rowIterator;
        }catch (Exception e){
            throw new RuntimeException("read StripeStatistics error");
        }

    }

    private Object getMinColumnValue(DataType dataType,OrcProto.ColumnStatistics statistics) {
        return getStatistics(dataType,statistics,true);
    }

    private Object getMaxColumnValue(DataType dataType,OrcProto.ColumnStatistics statistics) {
        return getStatistics(dataType,statistics,false);
    }
    /**
     * 创建reader
     * @return
     */
    protected Reader getORCReader(){
        try {
            Configuration conf = new Configuration();
            Reader reader = OrcFile.createReader(new Path(orcFilePath),
                OrcFile.readerOptions(conf));
            return reader;
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    /**
     * 把列值设置到orc的列对象中
     * @param vector 列对象
     * @param value 列值
     * @param rowIndex 行号
     */
    protected void setColumnValue(ColumnVector vector, Object value,int rowIndex) {
        try {
            if(BytesColumnVector.class.isInstance(vector)){
                BytesColumnVector bytesColumnVector=(BytesColumnVector)vector;
                byte[] bytes=null;
                if(value==null){
                    bytes=new byte[0];
                }else {
                    bytes=value.toString().getBytes("UTF-8");
                }
                bytesColumnVector.setVal(rowIndex, bytes);
            }else if(LongColumnVector.class.isInstance(vector)){
                LongColumnVector columnVector=(LongColumnVector)vector;
                columnVector.vector[rowIndex]=(Long)value;
            }else {
                throw new RuntimeException("can not support this columnVector "+vector.toString());
            }

            //todo other columnVector impl
        }catch (Exception e){
            throw new RuntimeException("set value 2 orc error",e);
        }

    }

    /**
     * 根据datatype转化成orc的格式
     * @param dataType
     * @return
     */
    protected TypeDescription createType(DataType dataType) {
        if(StringDataType.class.isInstance(dataType)){
            return TypeDescription.createString();
        }else if(IntDataType.class.isInstance(dataType)){
            return TypeDescription.createInt();
        }else if(LongDataType.class.isInstance(dataType)){
            return TypeDescription.createLong();
        }else if(DoubleDataType.class.isInstance(dataType)){
            return TypeDescription.createDouble();
        }else if(FloatDataType.class.isInstance(dataType)){
            return TypeDescription.createFloat();
        }else if(DateDataType.class.isInstance(dataType)){
            return TypeDescription.createDate();
        }else if(BooleanDataType.class.isInstance(dataType)){
            return TypeDescription.createBoolean();
        }else if(ListDataType.class.isInstance(dataType)){
            ListDataType listDataType=(ListDataType)dataType;
            return TypeDescription.createList(createType(listDataType.getParadigmType()));
        }else if(ShortDataType.class.isInstance(dataType)){
            return TypeDescription.createShort();
        }else if(ByteDataType.class.isInstance(dataType)){
            return TypeDescription.createByte();
        }else if(MapDataType.class.isInstance(dataType)){
            MapDataType mapDataType=(MapDataType)dataType;
            return TypeDescription.createMap(createType(mapDataType.getKeyParadigmType()),createType(mapDataType.getValueParadigmType()));
        }else {
            throw new RuntimeException("can not support this datatype "+dataType);
        }

    }

    /**
     * 根据datatype转化成orc的格式
     * @param dataType
     * @return
     */
    protected Object getStatistics(DataType dataType,OrcProto.ColumnStatistics statistics,boolean isMin) {
        if(StringDataType.class.isInstance(dataType)){
            return isMin?statistics.getStringStatistics().getMinimum():statistics.getStringStatistics().getMaximum();
        }else if(IntDataType.class.isInstance(dataType)||LongDataType.class.isInstance(dataType)||ShortDataType.class.isInstance(dataType)){
            return isMin?statistics.getIntStatistics().getMinimum():statistics.getIntStatistics().getMaximum();
        }else if(DoubleDataType.class.isInstance(dataType)||FloatDataType.class.isInstance(dataType)){
            return isMin?statistics.getDoubleStatistics().getMinimum():statistics.getDoubleStatistics().getMaximum();
        }else if(DateDataType.class.isInstance(dataType)){
            return isMin?statistics.getDateStatistics().getMinimum():statistics.getDateStatistics().getMaximum();
        }else {
            throw new RuntimeException("can not support this datatype "+dataType);
        }

    }


    protected boolean isMatch(DataType type, Object min, Object max, Object[] terms) {
        if(terms==null){
            return true;
        }

        for(Object term:terms){
            boolean isMatch=  TermMatchUtil.matchBetween(term,type,min,max);
            if(isMatch){
                return true;
            }
        }
        return false;
    }


    protected boolean isMatch(DataType type, Object min, Object max, Object queryMin, Object queryMax) {
       return isMatch(type,min,max,new Object[]{queryMin,queryMax});
    }

}
