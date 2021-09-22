package org.apache.rocketmq.streams.storage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.operator.impl.SessionOperator;
import org.apache.rocketmq.streams.window.state.WindowBaseValue;
import org.apache.rocketmq.streams.window.state.impl.WindowValue;
import org.apache.rocketmq.streams.window.storage.WindowStorage;
import org.apache.rocketmq.streams.window.storage.rocksdb.RocksdbStorage;
import org.junit.Assert;
import org.junit.Test;

public class RocksdbTest {

    @Test
    public void testMultiValues() {
        RocksdbStorage storage = new RocksdbStorage<>();
        //
        List<WindowBaseValue> valueList = new ArrayList<>();
        WindowBaseValue value1 = new WindowBaseValue();
        value1.setStartTime("2021-09-07 11:00:00");
        value1.setEndTime("2021-09-07 11:10:00");
        value1.setFireTime("2021-09-07 11:11:00");
        WindowBaseValue value2 = new WindowBaseValue();
        value2.setStartTime("2021-09-07 12:00:00");
        value2.setEndTime("2021-09-07 12:10:00");
        value2.setFireTime("2021-09-07 12:11:00");
        valueList.add(value1);
        valueList.add(value2);
        //
        String key = "test";
        Map<String, List<WindowBaseValue>> theMap = new HashMap<>();
        theMap.put(key, valueList);
        storage.multiPutList(theMap);
        Map<String, List<WindowBaseValue>> resultMap = storage.multiGetList(WindowBaseValue.class, new ArrayList<String>() {{
            add(key);
        }});
        Assert.assertEquals(1, resultMap.size());
        Assert.assertEquals(2, resultMap.get(key).size());
        Assert.assertEquals("2021-09-07 11:00:00", resultMap.get(key).get(0).getStartTime());
        Assert.assertEquals("2021-09-07 12:00:00", resultMap.get(key).get(1).getStartTime());
    }

    @Test
    public void testValueWithPrefix() {
        RocksdbStorage storage = new RocksdbStorage<>();
        //
        WindowInstance windowInstance = new WindowInstance();
        windowInstance.setStartTime(SessionOperator.SESSION_WINDOW_BEGIN_TIME);
        windowInstance.setEndTime(SessionOperator.SESSION_WINDOW_END_TIME);
        windowInstance.setFireTime("2021-09-07 12:00:00");
        //
        Map<String, WindowValue> valueMap = new HashMap<>();
        WindowValue value1 = new WindowValue();
        value1.setStartTime("2021-09-07 11:00:00");
        value1.setEndTime("2021-09-07 11:10:00");
        value1.setFireTime("2021-09-07 11:11:00");
        value1.setPartitionNum(100001);
        WindowValue value2 = new WindowValue();
        value2.setStartTime("2021-09-07 12:00:00");
        value2.setEndTime("2021-09-07 12:10:00");
        value2.setFireTime("2021-09-07 12:11:00");
        value2.setPartitionNum(100002);
        WindowValue value3 = new WindowValue();
        value3.setStartTime("2021-09-07 11:10:00");
        value3.setEndTime("2021-09-07 11:20:00");
        value3.setFireTime("2021-09-07 11:25:00");
        value3.setPartitionNum(100003);
        //
        String prefix = "sorted_session_window_key";
        String queueId = "001";
        String groupByValue = "default";
        String localPrefix = prefix + queueId;
        String sortKey1 = MapKeyUtil.createKey(localPrefix, windowInstance.createWindowInstanceId(), value1.getFireTime(), String.valueOf(value1.getPartitionNum()), groupByValue);
        String sortKey2 = MapKeyUtil.createKey(localPrefix, windowInstance.createWindowInstanceId(), value2.getFireTime(), String.valueOf(value2.getPartitionNum()), groupByValue);
        String sortKey3 = MapKeyUtil.createKey(localPrefix, windowInstance.createWindowInstanceId(), value3.getFireTime(), String.valueOf(value3.getPartitionNum()), groupByValue);
        valueMap.put(sortKey1, value1);
        valueMap.put(sortKey2, value2);
        valueMap.put(sortKey3, value3);
        storage.multiPut(valueMap);
        //
        WindowStorage.WindowBaseValueIterator<WindowValue> iterator = storage.loadWindowInstanceSplitData(prefix, queueId, windowInstance.createWindowInstanceId(), null, WindowValue.class);
        List<WindowValue> valueList = new ArrayList<>();
        while (iterator.hasNext()) {
            WindowValue value = iterator.next();
            valueList.add(value);
        }
        Assert.assertEquals(3, valueList.size());
        Assert.assertEquals("2021-09-07 11:25:00", valueList.get(1).getFireTime());
        //
        List<WindowValue> sortList = new ArrayList<>(valueMap.values());
        Collections.sort(sortList, Comparator.comparing(WindowValue::getStartTime));
        for (WindowValue value : sortList) {
            System.out.println(value.getStartTime() + " " + value.getEndTime() + " " + value.getFireTime());
        }
        //
        WindowValue value4 = new WindowValue();
        value4.setStartTime("2021-09-07 11:10:00");
        value4.setEndTime("2021-09-07 11:21:00");
        value4.setFireTime("2021-09-07 11:25:00");
        value4.setPartitionNum(100003);
        String sortKey4 = MapKeyUtil.createKey(localPrefix, windowInstance.createWindowInstanceId(), value4.getFireTime(), String.valueOf(value4.getPartitionNum()), groupByValue);
        valueMap.put(sortKey4, value4);
        storage.multiPut(valueMap);
        iterator = storage.loadWindowInstanceSplitData(prefix, queueId, windowInstance.createWindowInstanceId(), null, WindowValue.class);
        valueList = new ArrayList<>();
        while (iterator.hasNext()) {
            WindowValue value = iterator.next();
            valueList.add(value);
        }
        for (WindowValue value : valueList) {
            System.out.println(value.getStartTime() + " " + value.getEndTime() + " " + value.getFireTime() + " " + value.getPartitionNum());
        }

    }

}
