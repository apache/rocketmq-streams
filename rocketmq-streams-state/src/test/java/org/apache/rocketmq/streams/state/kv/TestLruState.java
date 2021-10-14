package org.apache.rocketmq.streams.state.kv;

import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.rocketmq.streams.state.LruState;
import org.junit.Assert;
import org.junit.Test;

public class TestLruState {

    @Test
    public void testBasic() {
        String content = "lru";
        int size = 100;
        final LruState<String> lruState = new LruState<>(10, "");
        //
        for (int i = 0; i < size; i++) {
            lruState.add(content);
        }
        Assert.assertEquals(size, lruState.search(content));
        Assert.assertEquals(1, lruState.count());
        lruState.remove(content);
        Assert.assertEquals(0, lruState.search(content));
        Assert.assertEquals(0, lruState.count());
        //
        for (int i = 0; i < 11; i++) {
            String value = content + "_" + i;
            for (int j = 0; j < i + 1; j++) {
                lruState.add(value);
            }
        }
        Iterator<String> iterator = lruState.iterator();
        int count = 0;
        String theValue = null;
        while (iterator.hasNext()) {
            count++;
            theValue = iterator.next();
            Assert.assertNotEquals(content + "_0", theValue);
        }
        Assert.assertEquals(content + "_1", theValue);
        Assert.assertEquals(10, count);
    }

    @Test
    public void testConcurrent() {
        String content = "lru";
        final LruState<String> lruState = new LruState<>(10, "");
        ExecutorService poolService = Executors.newFixedThreadPool(10);
        final Random random = new Random(System.currentTimeMillis());
        for (int i = 0; i < 1000; i++) {
            final int index = i;
            poolService.execute(() -> {
                lruState.add(content + random.nextInt(11));
            });
        }
        Assert.assertEquals(10, lruState.count());
    }
}


