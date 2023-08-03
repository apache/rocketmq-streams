package org.apache.rocketmq.streams.core.util;

import org.apache.rocketmq.streams.core.common.Constant;
import org.rocksdb.*;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Predicate;

public class ColumnFamilyUtil {

    public static final String WATERMARK_STATE_CF = "watermark-state";

    public static final String WINDOW_STATE_CF = "window-state";

    public static final String VALUE_STATE_CF = "value-state";

    private enum ColumnFamilyEnum {
        WATERMARK_STATE_COLUMN_FAMILY(WATERMARK_STATE_CF, str -> {
            return str.startsWith(Constant.WATERMARK_KEY);
        }),
        WINDOW_STATE_COLUMN_FAMILY(WINDOW_STATE_CF, str -> {
            String[] splits = str.split("&&");
            return splits.length == 4;
        }),
        VALUE_STATE_COLUMN_FAMILY(VALUE_STATE_CF, str -> true)
        ;

        private final String name;

        private final Predicate<String> keyCheckFunc;

        ColumnFamilyEnum(String name, Predicate<String> keyCheckFunc) {
            this.name = name;
            this.keyCheckFunc = keyCheckFunc;
        }

    }

    private final static Map<String, ColumnFamilyHandle> cFName2CFHandle = new HashMap<>();

    public static void createColumnFamilies(RocksDB rocksDB, ColumnFamilyOptions cfOpts) throws RocksDBException, UnsupportedEncodingException {
        final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
        for (ColumnFamilyEnum columnFamilyEnum : ColumnFamilyEnum.values()) {
            cfDescriptors.add(new ColumnFamilyDescriptor(columnFamilyEnum.name.getBytes(StandardCharsets.UTF_8), cfOpts));
        }
        List<ColumnFamilyHandle> columnFamilyHandles = rocksDB.createColumnFamilies(cfDescriptors);
        for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
            cFName2CFHandle.put(new String(columnFamilyHandle.getName(), StandardCharsets.UTF_8), columnFamilyHandle);
        }
    }

    public static String getColumnFamilyByKey(byte[] key) {
        if (key == null) {
            return null;
        }
        return getColumnFamilyByKey(new String(key, StandardCharsets.UTF_8));
    }

    public static String getColumnFamilyByKey(String key) {
        for (ColumnFamilyEnum columnFamilyEnum : ColumnFamilyEnum.values()) {
            if (columnFamilyEnum.keyCheckFunc.test(key)) {
                return columnFamilyEnum.name;
            }
        }
        return null;
    }

    public static ColumnFamilyHandle getColumnFamilyHandleByName(String name) {
        return cFName2CFHandle.get(name);
    }
}
