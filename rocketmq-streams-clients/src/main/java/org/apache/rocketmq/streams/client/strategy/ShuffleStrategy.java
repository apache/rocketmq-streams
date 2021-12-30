package org.apache.rocketmq.streams.client.strategy;

import org.apache.rocketmq.streams.common.component.AbstractComponent;

import java.util.Properties;

public class ShuffleStrategy implements Strategy {

    private final Properties properties;

    private ShuffleStrategy(String windowShuffleType) {
        properties = new Properties();
        properties.put(AbstractComponent.WINDOW_SHUFFLE_CHANNEL_TYPE, windowShuffleType);
    }

    @Override
    public Properties getStrategyProperties() {
        return this.properties;
    }

    public static Strategy shuffleWithMemory() {
        return new ShuffleStrategy("memory");
    }

}




