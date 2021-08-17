package org.apache.rocketmq.streams.client;

import org.apache.rocketmq.streams.common.channel.impl.file.FileSource;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;
import org.junit.Test;

public class SourceTest {

    @Test
    public void testSource(){
        FileSource source = new FileSource("/tmp/file.txt");
        source.setJsonData(true);
        source.init();
        source.start(new IStreamOperator() {
            @Override
            public Object doMessage(IMessage message, AbstractContext context) {
                System.out.println(message.getMessageBody());
                return null;
            }
        });
    }
}
