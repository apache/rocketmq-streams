package org.apache.rocketmq.streams.common.channel.impl.transit;

import com.alibaba.fastjson.JSONObject;
import com.google.auto.service.AutoService;
import java.util.Properties;
import org.apache.rocketmq.streams.common.channel.builder.IChannelBuilder;
import org.apache.rocketmq.streams.common.channel.impl.memory.MemorySource;
import org.apache.rocketmq.streams.common.channel.sink.ISink;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.model.ServiceName;
import org.apache.rocketmq.streams.common.utils.ConfigurableUtil;

@AutoService(IChannelBuilder.class)
@ServiceName(value = TransitChannelBuilder.TYPE, aliasName = "cache_table")
public class TransitChannelBuilder implements IChannelBuilder {
    public static final String TYPE = "transit";

    @Override public ISource createSource(String namespace, String name, Properties properties, MetaData metaData) {
        return (TransitSource)ConfigurableUtil.create(TransitSource.class.getName(), namespace, name, createFormatProperty(properties), null);
    }

    @Override public String getType() {
        return TYPE;
    }

    @Override public ISink createSink(String namespace, String name, Properties properties, MetaData metaData) {
        return (TransitSink)ConfigurableUtil.create(TransitSink.class.getName(), namespace, name, createFormatProperty(properties), null);
    }


    /**
     * 创建标准的属性文件
     *
     * @param properties
     * @return
     */
    protected JSONObject createFormatProperty(Properties properties) {
        JSONObject formatProperties = new JSONObject();
        for (Object object : properties.keySet()) {
            String key = (String)object;
            if ("type".equals(key)) {
                continue;
            }
            formatProperties.put(key, properties.getProperty(key));
        }
        IChannelBuilder.formatPropertiesName(formatProperties, properties, "logFingerprintFieldNames", "filterFieldNames");
        IChannelBuilder.formatPropertiesName(formatProperties, properties, "tableName", "sql_create_table_name");
//        IChannelBuilder.formatPropertiesName(formatProperties, properties, "maxThread", "thread.max.count");
        return formatProperties;
    }
}
