package org.apache.rocketmq.streams.client;

import java.util.Properties;
import org.apache.rocketmq.streams.common.configuration.ConfigurationKey;
import org.apache.rocketmq.streams.common.configuration.SystemContext;
import org.apache.rocketmq.streams.common.utils.StringUtil;

public class AbstractExecutionEnvironment<T extends AbstractExecutionEnvironment<?>> {

    private final Properties properties;

    protected AbstractExecutionEnvironment() {
        this.properties = new Properties();
        this.properties.putAll(SystemContext.getProperties());
    }

    public T db(String url, String userName, String password, String driver) {
        this.properties.put(ConfigurationKey.JDBC_URL, url);
        this.properties.put(ConfigurationKey.JDBC_USERNAME, userName);
        this.properties.put(ConfigurationKey.JDBC_PASSWORD, password);
        if (StringUtil.isNotEmpty(driver)) {
            this.properties.put(ConfigurationKey.JDBC_DRIVER, driver);
        }

        this.properties.put(ConfigurationKey.INTELLIGENCE_JDBC_URL, url);
        this.properties.put(ConfigurationKey.INTELLIGENCE_JDBC_USERNAME, userName);
        this.properties.put(ConfigurationKey.INTELLIGENCE_JDBC_PASSWORD, password);
        return (T) this;

    }

    /**
     * 配置情报的非db连接
     */
    public T intelligence(String endpoint, String ak, String sk, String region) {
        this.properties.put(ConfigurationKey.INTELLIGENCE_SWITCH, "true");
        this.properties.put(ConfigurationKey.INTELLIGENCE_TIP_DB_ENDPOINT, endpoint);
        this.properties.put(ConfigurationKey.INTELLIGENCE_AK, ak);
        this.properties.put(ConfigurationKey.INTELLIGENCE_SK, sk);
        this.properties.put(ConfigurationKey.INTELLIGENCE_REGION, region);
        return (T) this;
    }

    /**
     * 打开pipeline html monitor
     */
    public T dispatcherTime(int timeSecond) {
        this.properties.put(ConfigurationKey.DIPPER_DISPATCHER_SCHEDULE_TIME, timeSecond);
        return (T) this;

    }

    /**
     * 打开pipeline html monitor
     */
    public T regexEngine() {
        this.properties.put(ConfigurationKey.DIPPER_REGEX_ENGINE, "re2j");
        return (T) this;

    }

    public T miniIo(String endpoint, String ak, String sk, String dir) {
        this.properties.put(ConfigurationKey.FILE_TRANSPORT_AK, endpoint);
        this.properties.put(ConfigurationKey.FILE_TRANSPORT_SK, ak);
        this.properties.put(ConfigurationKey.FILE_TRANSPORT_ENDPOINT, sk);
        this.properties.put(ConfigurationKey.FILE_TRANSPORT_DIPPER_DIR, dir);
        return (T) this;
    }

    /**
     * 设置指纹缓存大小
     */

    public T cache(int rowSize) {
        this.properties.put(ConfigurationKey.FINGERPRINT_CACHE_SIZE, rowSize);
        return (T) this;
    }

    /**
     * 设置指纹缓存大小
     */

    public T monitor(String updateType, String updateTopic, String updateTag, String downTopic, String downTag, String ruleTopic) {
        this.properties.put(ConfigurationKey.UPDATE_TYPE, updateType);
        this.properties.put(ConfigurationKey.RULE_UP_TOPIC, updateTopic);
        this.properties.put(ConfigurationKey.RULE_UP_TAG, updateTag);
        this.properties.put(ConfigurationKey.RULE_DOWN_TOPIC, downTopic);
        this.properties.put(ConfigurationKey.RULE_DOWN_TAG, downTag);
        this.properties.put(ConfigurationKey.RULE_TOPIC_TAG, ruleTopic);
        return (T) this;
    }

    /**
     * 设置指纹缓存大小
     */

    public T httpRam(String endpoint, String ak, String sk) {
        this.properties.put(ConfigurationKey.HTTP_SERVICE_ENDPOINT, endpoint);
        this.properties.put(ConfigurationKey.HTTP_AK, ak);
        this.properties.put(ConfigurationKey.HTTP_SK, sk);
        return (T) this;
    }

    public Properties getProperties() {
        return properties;
    }
}
