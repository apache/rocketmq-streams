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
package org.apache.rocketmq.streams.storage;

import com.alibaba.fastjson.JSONObject;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.apache.rocketmq.streams.common.cache.compress.impl.KeySet;
import org.apache.rocketmq.streams.common.channel.impl.file.FileSource;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;
import org.apache.rocketmq.streams.common.threadpool.ThreadPoolFactory;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.junit.Test;

public class BloomFilterTest {
    protected static String fieldNames =
        "aliUid\n" +
            "buyAegis\n" +
            "buySas\n" +
            "client_mode\n" +
            "cmd_chain\n" +
            "cmd_chain_index\n" +
            "cmd_index\n" +
            "cmdline\n" +
            "comm\n" +
            "containerhostname\n" +
            "containermip\n" +
            "containername\n" +
            "cwd\n" +
            "data_complete\n" +
            "delta_t1\n" +
            "delta_t2\n" +
            "docker_file_path\n" +
            "dockercontainerid\n" +
            "dockerimageid\n" +
            "dockerimagename\n" +
            "egroup_id\n" +
            "egroup_name\n" +
            "euid\n" +
            "euid_name\n" +
            "file_gid\n" +
            "file_gid_name\n" +
            "file_name\n" +
            "file_path\n" +
            "file_uid\n" +
            "file_uid_name\n" +
            "gcLevel\n" +
            "gid\n" +
            "gid_name\n" +
            "host_uuid\n" +
            "index\n" +
            "k8sclusterid\n" +
            "k8snamespace\n" +
            "k8snodeid\n" +
            "k8snodename\n" +
            "k8spodname\n" +
            "logTime\n" +
            "log_match\n" +
            "parent_cmd_line\n" +
            "parent_file_name\n" +
            "parent_file_path\n" +
            "pcomm\n" +
            "perm\n" +
            "pid\n" +
            "pid_start_time\n" +
            "ppid\n" +
            "scan_time\n" +
            "sid\n" +
            "srv_cmd\n" +
            "stime\n" +
            "tty\n" +
            "uid\n" +
            "uid_name\n";

    @Test
    public void testBloomFilter() throws IOException, InterruptedException {
        long start = System.currentTimeMillis();
        String[] columnNames = new String[] {"aliUid", "cmdline", "file_name", "file_path", "parent_cmd_line", "parent_file_name", "parent_file_path", "host_uuid"};//fieldNames.split("\n");
        FileSource fileSource = new FileSource("/Users/yuanxiaodong/aegis_proc_public_1G.txt");
        fileSource.init();
        BloomFilter<String> filter = BloomFilter.create(
            Funnels.stringFunnel(Charset.defaultCharset()),
            335544320,
            0.3);
        AtomicInteger count = new AtomicInteger(0);
        ExecutorService executorService = ThreadPoolFactory.createThreadPool(10, BloomFilterTest.class.getName() + "-index");
        KeySet keySet = new KeySet(580000 * columnNames.length);
        Object object = this;
        fileSource.start(new IStreamOperator() {
            @Override public Object doMessage(IMessage message, AbstractContext context) {
                JSONObject jsonObject = message.getMessageBody();
                count.incrementAndGet();
                if (count.get() % 1000 == 0) {
                    System.out.println("progress " + (double) count.get() / (double) 580000);
                }
                for (int i = 0; i < columnNames.length; i++) {
                    String word = jsonObject.getString(columnNames[i]);
                    if (StringUtil.isEmpty(word)) {
                        continue;
                    }
                    if (keySet.contains(word)) {
                        continue;
                    } else {
                        keySet.add(word);
                    }
                    Set<String> splitsWords = null;
                    try {
                        splitsWords = splitWords(word);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    for (String w : splitsWords) {
                        filter.put(w);
                    }
                }

//                executorService.execute(new Runnable() {
//                    @Override public void run() {
//
//                    }
//                });
                return null;
            }
        });
        filter.writeTo(new FileOutputStream("/Users/yuanxiaodong/orc_1/bloom_test_3"));
        System.out.println("cost is:" + (System.currentTimeMillis() - start));

//
//
//
//        long startTime=System.currentTimeMillis();
//filter =BloomFilter.readFrom(new BufferedInputStream(new FileInputStream("/tmp/orc/bloom_test_3")),Funnels.stringFunnel(Charset.defaultCharset()));
//        System.out.println(filter.mightContain("fdsfsdfdsdfds"));
//
//        String cmdline="/bash/dat33";
//       words=splitWords(cmdline);
//       boolean isMatch=true;
//        for(String word:words){
//            if(!filter.mightContain(word)){
//                isMatch=false;
//                break;
//            }
//        }
//        System.out.println(isMatch);
        //        System.out.println(filter.mightContain(cmdline));
//
//        System.out.println(filter.mightContain(cmdline.substring(0,10)));
//
//        System.out.println(filter.mightContain(cmdline.substring(cmdline.length()-10)));

//        System.out.println("cost is "+(System.currentTimeMillis()-startTime));
    }

    @Test
    public void testBloom() throws IOException {
        BloomFilter filter = BloomFilter.readFrom(new BufferedInputStream(new FileInputStream("/Users/yuanxiaodong/orc_1/bloom_test_3")), Funnels.stringFunnel(Charset.defaultCharset()));
        System.out.println(filter.mightContain(StringUtil.createMD5Str("java")));
    }

    private Set<String> splitWords(String text) throws IOException {
        if (StringUtil.isEmpty(text)) {
            return new HashSet<>();
        }
        Set<String> words = new HashSet<>();
        if (text.length() < 3) {
            words.add(text);
            return words;
        }
        StringReader reader = new StringReader(text);
        NGramTokenizer gramTokenizer = new NGramTokenizer(Version.LUCENE_47, reader, 3, 10);
        CharTermAttribute charTermAttribute = gramTokenizer.addAttribute(CharTermAttribute.class);
        gramTokenizer.reset();

        while (gramTokenizer.incrementToken()) {
            String token = charTermAttribute.toString();
            words.add(token);

        }
        gramTokenizer.end();
        gramTokenizer.close();
        return words;
    }

    @Test
    public void testWords() throws IOException {
        Set<String> words = splitWords("%bash%/dat33%");
        for (String word : words) {
            System.out.println(word);
        }
    }
}
