///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package org.apache.rocketmq.streams.core.function.supplier;
//
//import org.apache.rocketmq.streams.core.function.ValueJoinAction;
//import org.apache.rocketmq.streams.core.running.AbstractProcessor;
//import org.apache.rocketmq.streams.core.running.Processor;
//
//import java.util.function.Supplier;
//
//public class JoinSupplier<V1, V2, OUT> implements Supplier<Processor<V1>> {
//    private final ValueJoinAction<V1, V2, OUT> joinAction;
//
//    public JoinSupplier(ValueJoinAction<V1, V2, OUT> joinAction) {
//        this.joinAction = joinAction;
//    }
//
//    @Override
//    public Processor<V1> get() {
//        return new JoinProcessorImpl(joinAction);
//    }
//
//    public class JoinProcessorImpl extends AbstractProcessor<V1> implements JoinProcessor<V1,V2>{
//        private final ValueJoinAction<V1, V2, OUT> joinAction;
//
//        public JoinProcessorImpl(ValueJoinAction<V1, V2, OUT> joinAction) {
//            this.joinAction = joinAction;
//        }
//
//        @Override
//        public void process(V1 data) throws Throwable {
//            throw new UnsupportedOperationException();
//        }
//
//        @Override
//        public void process(V1 o1, V2 o2) throws Throwable {
//            OUT apply = joinAction.apply(o1, o2);
//            V1 convert = super.convert(apply);
//            this.context.forward(convert);
//        }
//    }
//
////    public interface JoinProcessor<V1, V2>{
////        void process(V1 o1, V2 o2) throws Throwable;
////    }
//}
