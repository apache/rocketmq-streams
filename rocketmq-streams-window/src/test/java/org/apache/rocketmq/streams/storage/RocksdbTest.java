package org.apache.rocketmq.streams.storage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.window.state.WindowBaseValue;
import org.apache.rocketmq.streams.window.storage.rocksdb.RocksdbStorage;
import org.junit.Assert;
import org.junit.Test;

public class RocksdbTest {

    @Test
    public void testMultiValues() {
        RocksdbStorage storage = new RocksdbStorage<WindowBaseValue>();
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

}
