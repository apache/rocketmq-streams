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
package org.apache.rocketmq.streams.connectors.source;

import com.alibaba.fastjson.JSON;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.checkpoint.CheckPoint;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.model.Pipeline;
import org.apache.rocketmq.streams.common.topology.task.TaskAssigner;
import org.apache.rocketmq.streams.common.utils.DipperThreadLocalUtil;
import org.apache.rocketmq.streams.common.utils.RuntimeUtil;
import org.apache.rocketmq.streams.connectors.model.PullMessage;
import org.apache.rocketmq.streams.connectors.reader.ISplitReader;
import org.apache.rocketmq.streams.connectors.reader.SplitCloseFuture;

public class MutilBatchTaskSource extends AbstractPullSource {

    @Override protected ISplitReader createSplitReader(ISplit split) {
        return new ISplitReader() {
            protected transient ISplit split;
            protected boolean isInterrupt;
            protected boolean isClose;
            protected transient AtomicLong offsetGenerator=new AtomicLong(1000000000);
            @Override public void open(ISplit split) {
                this.split=split;
            }

            @Override public boolean next() {
                return true;
            }

            @Override public List<PullMessage> getMessage() {
                PipelineSplit pipelineSplit=(PipelineSplit)split;
                pipelineSplit.getQueue().startChannel();
                return null;
            }

            @Override public SplitCloseFuture close() {
                isClose=true;
                return new SplitCloseFuture(this,split);
            }

            @Override public void seek(String cursor) {

            }

            @Override public String getProgress() {
                return RuntimeUtil.getDipperInstanceId()+"_"+offsetGenerator.incrementAndGet();
            }

            @Override public long getDelay() {
                return 0;
            }

            @Override public long getFetchedDelay() {
                return getPullIntervalMs();
            }

            @Override public boolean isClose() {
                return isClose;
            }

            @Override public ISplit getSplit() {
                return split;
            }

            @Override public boolean isInterrupt() {
                return isInterrupt;
            }

            @Override public boolean interrupt() {
                isInterrupt=true;
                return isInterrupt;
            }
        };
    }

    @Override protected boolean isNotDataSplit(String queueId) {
        return false;
    }

    @Override public List<ISplit> fetchAllSplits() {

        List<TaskAssigner> taskAssigners = configurableService.queryConfigurableByType(TaskAssigner.TYPE);
        if (taskAssigners == null) {
            return null;
        }
        String taskName = getConfigureName();
        List<ISplit> splits=new ArrayList<>();
        for (TaskAssigner taskAssigner : taskAssigners) {
            if (!taskName.equals(taskAssigner.getTaskName())) {
                continue;
            }
            String pipelineName = taskAssigner.getPipelineName();
            if(pipelineName!=null){
                ChainPipeline<?> pipeline = configurableService.queryConfigurable(Pipeline.TYPE, pipelineName);
                if (pipeline != null) {
                    splits.add(new PipelineSplit(pipeline));
                }
            }
        }
        return splits;
    }


    protected class PipelineSplit implements ISplit<PipelineSplit, ChainPipeline>{
        protected ChainPipeline chainPipeline;
        public PipelineSplit(ChainPipeline chainPipeline){
            this.chainPipeline=chainPipeline;
        }

        @Override public String getQueueId() {
            return chainPipeline.getConfigureName();
        }

        @Override public ChainPipeline getQueue() {
            return chainPipeline;
        }

        @Override public int compareTo(PipelineSplit o) {
            return chainPipeline.getConfigureName().compareTo(o.getQueueId());
        }

        @Override public String toJson() {
            return chainPipeline.toJson();
        }

        @Override public void toObject(String jsonString) {
            ChainPipeline pipeline=new ChainPipeline();
            pipeline.toObject(jsonString);
            this.chainPipeline=pipeline;
        }
    }

    @Override
    public String loadSplitOffset(ISplit split) {
        return null;
    }

}
