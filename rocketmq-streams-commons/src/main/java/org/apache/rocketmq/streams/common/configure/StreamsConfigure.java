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
package org.apache.rocketmq.streams.common.configure;

import org.apache.rocketmq.streams.common.component.ComponentCreator;

public class StreamsConfigure {
    protected static String EMIT_BEFORE_VALUE_KEY="emit.before.value";
    protected static String EMIT_AFTER_VALUE_KEY="emit.after.value";
    protected static String EMIT_MAX_DELAY_VALUE_KEY="emit.max.value";
    public static Long getEmitBeforeValue(){
        String value= ComponentCreator.getProperties().getProperty(EMIT_BEFORE_VALUE_KEY);
        if(value==null){
            return null;
        }
        return Long.valueOf(value);
    }
    public static void setEmitBeforeValue(long value){
        ComponentCreator.getProperties().put(EMIT_BEFORE_VALUE_KEY,value+"");
    }
    public static void setEmitAfterValue(long value){
        ComponentCreator.getProperties().put(EMIT_AFTER_VALUE_KEY,value+"");
    }

    public static Long getEmitAfterValue(){
        String value= ComponentCreator.getProperties().getProperty(EMIT_AFTER_VALUE_KEY);
        if(value==null){
            return null;
        }
        return Long.valueOf(value);
    }

    public static void setEmitMaxDelayValueKey(long value){
        ComponentCreator.getProperties().put(EMIT_MAX_DELAY_VALUE_KEY,value+"");
    }

    public static Long getEmitMaxDelay(){
        String value= ComponentCreator.getProperties().getProperty(EMIT_MAX_DELAY_VALUE_KEY);
        if(value==null){
            return null;
        }
        return Long.valueOf(value);
    }

}