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
package org.apache.rocketmq.streams.dim.builder;

import com.google.auto.service.AutoService;
import java.util.Properties;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.model.ServiceName;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.dim.model.AbstractDim;
import org.apache.rocketmq.streams.dim.model.FileDim;

@AutoService(IDimSQLParser.class)
@ServiceName(value = FileDimSQLParser.TYPE, aliasName = "FILE")
public class FileDimSQLParser extends AbstractDimParser{
    public static final String TYPE="file";

    @Override
    protected AbstractDim createDim(Properties properties, MetaData data) {
        String filePath=properties.getProperty("filePath");
        if(StringUtil.isEmpty(filePath)){
            filePath=properties.getProperty("file_path");
        }
        FileDim fileDim=new FileDim();
        fileDim.setFilePath(filePath);
        return fileDim;
    }
}
