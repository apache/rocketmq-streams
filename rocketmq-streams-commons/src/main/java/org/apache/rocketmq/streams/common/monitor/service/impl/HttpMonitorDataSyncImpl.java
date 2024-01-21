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
package org.apache.rocketmq.streams.common.monitor.service.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.http.HttpEntity;
import org.apache.http.ParseException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.ContentType;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.Args;
import org.apache.http.util.CharArrayBuffer;
import org.apache.rocketmq.streams.common.monitor.HttpUtil;
import org.apache.rocketmq.streams.common.monitor.model.JobStage;
import org.apache.rocketmq.streams.common.monitor.model.TraceIdsDO;
import org.apache.rocketmq.streams.common.monitor.model.TraceMonitorDO;
import org.apache.rocketmq.streams.common.monitor.service.MonitorDataSyncService;

public class HttpMonitorDataSyncImpl implements MonitorDataSyncService {

    public static final String GET_TRACE_IDS = "/queryValidTraceId";
    public static final String UPDATE_JOBSTAGE = "/updateJobStage";
    public static final String ADD_TRACEMONITOR = "/insertTraceMonitor";
    protected String endPoint;
    protected HttpUtil client;

    public HttpMonitorDataSyncImpl(String accessId, String accessIdSecret, String endPoint) {
        client = new HttpUtil(accessId, accessIdSecret, endPoint);
        this.endPoint = endPoint;
    }

    @Override
    public List<TraceIdsDO> getTraceIds() {
        List<TraceIdsDO> traceIdsDOS = new ArrayList<>();
        CloseableHttpResponse response = client.get(endPoint + GET_TRACE_IDS, null);
        if (response != null && response.getStatusLine().getStatusCode() == 200) {
            traceIdsDOS = convert(response, TraceIdsDO.class);
        }

        return traceIdsDOS;
    }

    @Override
    public void updateJobStage(Collection<JobStage> jobStages) {

        String response = client.postContent(endPoint + UPDATE_JOBSTAGE, JSONObject.toJSONString(jobStages));

    }

    @Override
    public void addTraceMonitor(TraceMonitorDO traceMonitorDO) {
        String response = client.postContent(endPoint + ADD_TRACEMONITOR, JSONObject.toJSONString(traceMonitorDO));
    }

    private <T> List<T> convert(CloseableHttpResponse response, Class clazz) {
        try {
            String content = toString(response.getEntity(), Charset.forName("UTF-8"));
            JSONObject object = JSONObject.parseObject(content);
            String data = object.getString("data");
            List<T> result = JSONArray.parseArray(data, clazz);
            return result;

        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public String toString(HttpEntity entity, Charset defaultCharset) throws IOException, ParseException {
        Args.notNull(entity, "Entity");
        InputStream instream = entity.getContent();
        if (instream == null) {
            return null;
        } else {
            try {
                Args.check(entity.getContentLength() <= 2147483647L, "HTTP entity too large to be buffered in memory");
                int i = (int) entity.getContentLength();
                if (i < 0) {
                    i = 4096;
                }

                Charset charset = null;

                try {
                    ContentType contentType = ContentType.get(entity);
                    if (contentType != null) {
                        charset = contentType.getCharset();
                    }
                } catch (UnsupportedCharsetException var13) {
                    if (defaultCharset == null) {
                        throw new UnsupportedEncodingException(var13.getMessage());
                    }
                }

                if (charset == null) {
                    charset = defaultCharset;
                }

                if (charset == null) {
                    charset = HTTP.DEF_CONTENT_CHARSET;
                }

                Reader reader = new InputStreamReader(instream, charset);
                CharArrayBuffer buffer = new CharArrayBuffer(i);
                char[] tmp = new char[1024];

                int l;
                while ((l = reader.read(tmp)) != -1) {
                    buffer.append(tmp, 0, l);
                }

                String var9 = buffer.toString();
                return var9;
            } finally {
                instream.close();
            }
        }
    }

}
