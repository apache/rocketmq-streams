/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.streams.core.function;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.rocketmq.streams.core.window.WindowState;
import org.apache.rocketmq.streams.core.Num;
import org.apache.rocketmq.streams.core.User;
import org.apache.rocketmq.streams.core.util.Utils;

import java.nio.charset.StandardCharsets;

public class AggregateSupplier {
    public static void main(String[] args) throws Throwable {
        WindowState<Num, User> state = new WindowState<>();
        Num num = new Num();
        num.setNumber(10);
        User user = new User();
        user.setName("zeni");
        state.setKey(num);
        state.setValue(user);

        byte[] bytes = object2Byte(state);

        WindowState<Num, User> result = byte2Object(bytes);

        System.out.println(result);
    }
    private static final ByteBuf buf = Unpooled.buffer(16);
    protected static byte[] object2Byte(Object obj) throws JsonProcessingException {
        if (obj == null) {
            return new byte[]{};
        }

        String name = obj.getClass().getName();
        byte[] className = name.getBytes(StandardCharsets.UTF_8);
        byte[] objBytes = Utils.object2Byte(obj);


        buf.writeInt(className.length);
        buf.writeBytes(className);
        buf.writeInt(objBytes.length);
        buf.writeBytes(objBytes);


        byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);

        buf.clear();
        buf.release();
        return bytes;
    }

    /**
     * decode
     * <pre>
     * +-----------+---------------+-------------+-------------+
     * | Int(4)    |   className  | Int(4)       | value bytes |
     * | classname |              |object length |             |
     * +-----------+--------------+---------------+-------------+
     * </pre>
     */
    @SuppressWarnings("unchecked")
    public static  <V> V byte2Object(byte[] bytes) throws Throwable {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(bytes);

        int classNameLength = byteBuf.readInt();
        ByteBuf classNameBuf = byteBuf.readBytes(classNameLength);

        byte[] clazzNameBytes = new byte[classNameBuf.readableBytes()];
        classNameBuf.readBytes(clazzNameBytes);
        //实例化
        String className = new String(clazzNameBytes, StandardCharsets.UTF_8);
        Class<V> clazz = (Class<V>)Class.forName(className);

        int objectLength = byteBuf.readInt();
        ByteBuf objBuf = byteBuf.readBytes(objectLength);
        byte[] objectBytes = new byte[objectLength];
        objBuf.readBytes(objectBytes);

        return Utils.byte2Object(objectBytes, clazz);
    }

}
