/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.rocketmq.streams.examples.rocketmqsource;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class ProducerFromFile {

    public static void produce(String filePath, String nameServ, String topic) {
        try {
            DefaultMQProducer producer = new DefaultMQProducer("test-group");
            producer.setNamesrvAddr(nameServ);
            producer.start();

            List<String> result = ProducerFromFile.read(filePath);

            for (String str : result) {
                Message msg = new Message(topic, "", str.getBytes(RemotingHelper.DEFAULT_CHARSET));
                SendResult sendResult = producer.send(msg);
                System.out.printf("%s%n", sendResult);
            }
            //Shut down once the producer instance is not longer in use.
            producer.shutdown();
        } catch (Throwable t) {
            t.printStackTrace();
        }

    }

    private static File getFile(String filePath) {
        File file = new File(filePath);
        if (!file.exists()) {
            ClassLoader loader = ProducerFromFile.class.getClassLoader();
            URL url = loader.getResource(filePath);

            if (url != null) {
                String path = url.getFile();
                file = new File(path);
            }
        }
        return file;

    }

    public static List<String> read(String path) {
        File file = getFile(path);
        List<String> result = new ArrayList<>();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));

            String line = reader.readLine();
            while (line != null && !"".equals(line)) {
                result.add(line);
                line = reader.readLine();
            }

        } catch (Throwable t) {
            t.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return result;
    }
}
