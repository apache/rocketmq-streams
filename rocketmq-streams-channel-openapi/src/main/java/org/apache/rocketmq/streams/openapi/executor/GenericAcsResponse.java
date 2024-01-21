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

import com.aliyuncs.AcsResponse;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.exceptions.ServerException;
import com.aliyuncs.http.HttpResponse;
import com.aliyuncs.transform.UnmarshallerContext;
import java.io.UnsupportedEncodingException;

public class GenericAcsResponse extends AcsResponse {

    private String content;

    public GenericAcsResponse() {
    }

    @Override
    public GenericAcsResponse getInstance(UnmarshallerContext context) throws ClientException, ServerException {
        try {
            HttpResponse httpResponse = context.getHttpResponse();
            if (httpResponse.getEncoding() == null) {
                this.content = new String(httpResponse.getHttpContent());
            } else {
                this.content = new String(httpResponse.getHttpContent(), httpResponse.getEncoding());
            }
            return this;
        } catch (UnsupportedEncodingException e) {
            throw new ClientException("SDK.UnsupportedEncoding", "Can not parse response due to un supported encoding.");
        }
    }

    public String getContent() {
        return this.content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}