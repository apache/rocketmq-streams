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

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.apache.rocketmq.streams.common.utils.RuntimeUtil;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.TtlDB;
import org.rocksdb.WriteOptions;

public class RocksDBOperator {

    protected static String DB_PATH = "/tmp/rocksdb";

    protected static String UTF8 = "UTF8";

    protected static AtomicBoolean hasCreate = new AtomicBoolean(false);

    protected static RocksDB rocksDB;

    static {
        RocksDB.loadLibrary();
    }

    protected WriteOptions writeOptions = new WriteOptions();

    public RocksDBOperator() {
        this(FileUtil.concatFilePath(DB_PATH + File.separator + RuntimeUtil.getDipperInstanceId(), "rocksdb"));
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
                                    //                                   final Filter bloomFilter = new BloomFilter(10);
//                                    final ReadOptions readOptions = new ReadOptions().setFillCache(false);
//                                    final Statistics stats = new Statistics();
//                                    final RateLimiter rateLimiter = new RateLimiter(10000000, 10000, 10);
//
//                                    options.setCreateIfMissing(true)
//                                      //  .setStatistics(stats)
//                                        .setWriteBufferSize(64*1024 * SizeUnit.KB)
//                                        .setMaxWriteBufferNumber(3);
//                                        .setMaxBackgroundJobs(10)
//                                        .setCompressionType(CompressionType.SNAPPY_COMPRESSION)
//                                        .setCompactionStyle(CompactionStyle.UNIVERSAL);
//
//                                  final BlockBasedTableConfig table_options = new BlockBasedTableConfig();
//                                   Cache cache = new LRUCache(10 * 1024, 6);
//                                    table_options.setBlockCache(cache)
//                                        .setFilterPolicy(bloomFilter);
//                                        .setBlockSizeDeviation(5)
//                                        .setBlockRestartInterval(10)
//                                        .setCacheIndexAndFilterBlocks(true);
//                                    //    .setBlockCacheCompressed(new LRUCache(64 * 1000, 10));
//                                   options.setTableFormatConfig(table_options);
//
//                                    options.setRateLimiter(rateLimiter);
                                    final TtlDB db = TtlDB.open(options, rocksdbFilePath, 10800, false);
                                    RocksDBOperator.rocksDB = db;
                                    writeOptions.setSync(false);
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
