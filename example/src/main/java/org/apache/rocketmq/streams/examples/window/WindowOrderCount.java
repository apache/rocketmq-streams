package org.apache.rocketmq.streams.examples.window;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.streams.core.RocketMQStream;
import org.apache.rocketmq.streams.core.function.SelectAction;
import org.apache.rocketmq.streams.core.metadata.StreamConfig;
import org.apache.rocketmq.streams.core.rstream.StreamBuilder;
import org.apache.rocketmq.streams.core.topology.TopologyBuilder;
import org.apache.rocketmq.streams.core.util.Pair;
import org.apache.rocketmq.streams.core.window.Time;
import org.apache.rocketmq.streams.core.window.WindowBuilder;
import org.apache.rocketmq.streams.examples.pojo.Order;

import java.util.Properties;

public class WindowOrderCount {
    public static void main(String[] args) {
        StreamBuilder builder = getOrder2();

        TopologyBuilder topologyBuilder = builder.build();
        Properties properties = new Properties();
        properties.putIfAbsent(MixAll.NAMESRV_ADDR_PROPERTY, "127.0.0.1:9876");
        properties.put(StreamConfig.ALLOW_LATENESS_MILLISECOND, 2000);


        RocketMQStream rocketMQStream = new RocketMQStream(topologyBuilder, properties);

        Runtime.getRuntime().addShutdownHook(new Thread("ordercount-shutdown-hook") {
            @Override
            public void run() {
                rocketMQStream.stop();
            }
        });

        rocketMQStream.start();
    }

    private static StreamBuilder getOrder1() {
        /**
         * Get the count of drink/food orders in last 30 seconds every 10 seconds
         **/
        StreamBuilder builder = new StreamBuilder("windowOrderCount");

        builder.source("order", source -> {
                    Order order = JSON.parseObject(source, Order.class);
                    System.out.println(order.toString());
                    return new Pair<>(null, order);
                })
                .keyBy(Order::getType)
                .window(WindowBuilder.slidingWindow(Time.seconds(30), Time.seconds(10)))
                .count()
                .toRStream()
                .print();

        return builder;
    }

    private static StreamBuilder getOrder2() {
        /**
         * Get how much the customers pay for drink/food every 100 seconds
         **/
        StreamBuilder builder = new StreamBuilder("windowOrderCount");
        builder.source("order", source -> {
                    Order order = JSON.parseObject(source, Order.class);
                    System.out.println(order.toString());
                    return new Pair<>(null, order);
                })
                .keyBy(new SelectAction<String, Order>() {
                    @Override
                    public String select(Order order) {
                        return order.getCustomer() + "@" + order.getType();
                    }
                })
                .window(WindowBuilder.tumblingWindow(Time.seconds(100)))
                .sum(Order::getPrice)
                .toRStream()
                .print();

        return builder;
    }
}
