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
package org.apache.rocketmq.streams.openapi.executor;

import com.aliyuncs.RpcAcsRequest;
import com.aliyuncs.http.FormatType;

public class GenericRpcAcsRequest extends RpcAcsRequest<GenericAcsResponse> {

    public GenericRpcAcsRequest(String product) {
        super(product);
        this.setAcceptFormat(FormatType.JSON);
    }

    public GenericRpcAcsRequest(String product, String version) {
        super(product);
        this.setAcceptFormat(FormatType.JSON);
    }

    public GenericRpcAcsRequest(String product, String version, String action) {
        super(product, version, action);
        this.setAcceptFormat(FormatType.JSON);
    }

    public GenericRpcAcsRequest(String product, String version, String action, String locationProduct) {
        super(product, version, action, locationProduct);
        this.setAcceptFormat(FormatType.JSON);
    }

    public GenericRpcAcsRequest(String product, String version, String action, String locationProduct, String endpointType) {
        super(product, version, action, locationProduct, endpointType);
        this.setAcceptFormat(FormatType.JSON);
    }

    @Override
    public Class<GenericAcsResponse> getResponseClass() {
        return GenericAcsResponse.class;
    }
}