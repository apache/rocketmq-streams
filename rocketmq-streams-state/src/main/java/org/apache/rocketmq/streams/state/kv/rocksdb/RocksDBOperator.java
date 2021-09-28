package org.apache.rocketmq.streams.state.kv.rocksdb;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.apache.rocketmq.streams.common.utils.RuntimeUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
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
