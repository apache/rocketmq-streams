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
package org.apache.rocketmq.streams.state.kv.rocksdb;

import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.apache.rocketmq.streams.common.utils.RuntimeUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.rocksdb.*;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

public class RocksDBOperator {

    protected static String DB_PATH = "/tmp/rocksdb";

    protected static String UTF8 = "UTF8";

    protected static AtomicBoolean hasCreate = new AtomicBoolean(false);

    protected static RocksDB rocksDB;

    protected WriteOptions writeOptions = new WriteOptions();

    static {
        RocksDB.loadLibrary();
    }

    public RocksDBOperator() {
        this(FileUtil.concatFilePath(StringUtil.isEmpty(FileUtil.getJarPath()) ? DB_PATH + File.separator + RuntimeUtil.getDipperInstanceId() : FileUtil.getJarPath() + File.separator + RuntimeUtil.getDipperInstanceId(), "rocksdb"));
    }

    public RocksDBOperator(String rocksdbFilePath) {
        if (hasCreate.compareAndSet(false, true)) {
            synchronized (RocksDBOperator.class) {
                if (RocksDBOperator.rocksDB == null) {
                    synchronized (RocksDBOperator.class) {
                        if (RocksDBOperator.rocksDB == null) {
                            try (final Options options = new Options().setCreateIfMissing(true)) {

                                try {
                                    File dir = new File(rocksdbFilePath);
                                    if (dir.exists()) {
                                        dir.delete();
                                    }
                                    dir.mkdirs();
                                    final TtlDB db = TtlDB.open(options, rocksdbFilePath, 10800, false);
                                    RocksDBOperator.rocksDB = db;
                                    writeOptions.setSync(true);
                                } catch (RocksDBException e) {
                                    throw new RuntimeException("create rocksdb error " + e.getMessage());
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    public RocksDB getInstance() {
        if (rocksDB == null) {
            synchronized (RocksDBOperator.class) {
                if (rocksDB == null) {
                    RocksDBOperator operator = new RocksDBOperator();
                    if (rocksDB != null) {
                        return rocksDB;
                    } else {
                        throw new RuntimeException("failed in creating rocksdb!");
                    }
                }
            }
        }
        return rocksDB;
    }

}
