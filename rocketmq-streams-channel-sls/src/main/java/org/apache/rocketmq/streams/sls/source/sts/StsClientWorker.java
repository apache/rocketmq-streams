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
package org.apache.rocketmq.streams.sls.source.sts;

import com.aliyun.openservices.loghub.client.ClientWorker;
import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import com.aliyun.openservices.loghub.client.exceptions.LogHubClientWorkerException;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessorFactory;
import java.util.concurrent.ExecutorService;
import org.apache.rocketmq.streams.sts.StsService;

public class StsClientWorker extends ClientWorker {

    transient StsService stsService;

    public StsClientWorker(ILogHubProcessorFactory factory, LogHubConfig config) throws LogHubClientWorkerException {
        super(factory, config);
    }

    public StsClientWorker(ILogHubProcessorFactory factory, LogHubConfig config, ExecutorService service) throws LogHubClientWorkerException {
        super(factory, config, service);
    }

    public void setStsService(StsService stsService) {
        this.stsService = stsService;
    }

    @Override
    public void run() {
        try {
            super.run();
        } catch (Exception e) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException exception) {
                exception.printStackTrace();
            }
            super.SwitchClient(null, null, null);
            super.run();

        }
    }

}
