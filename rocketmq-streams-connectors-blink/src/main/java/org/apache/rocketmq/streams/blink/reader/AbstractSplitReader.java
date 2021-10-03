//package org.apache.rocketmq.streams.blink.reader;
//
//import com.alibaba.blink.streaming.connectors.common.reader.RecordReader;
//import org.apache.flink.api.common.ExecutionConfig;
//import org.apache.flink.api.common.accumulators.*;
//import org.apache.flink.api.common.cache.DistributedCache;
//import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
//import org.apache.flink.api.common.functions.RuntimeContext;
//import org.apache.flink.api.common.state.*;
//import org.apache.flink.core.io.InputSplit;
//import org.apache.flink.metrics.MetricGroup;
//import org.apache.rocketmq.streams.common.channel.split.ISplit;
//import org.apache.rocketmq.streams.connectors.model.PullMessage;
//import org.apache.rocketmq.streams.connectors.reader.ISplitReader;
//import org.apache.rocketmq.streams.connectors.reader.SplitCloseFuture;
//
//import java.io.IOException;
//import java.io.Serializable;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.CompletableFuture;
//
//public abstract class AbstractSplitReader<OUT, CURSOR extends Serializable> implements ISplitReader {
//
//    protected transient RecordReader<OUT,CURSOR> recordReader;
//    protected volatile boolean isInterrupt=false;
//    protected volatile boolean isClose=false;
//
//    protected transient ISplit split;
//
//    protected AbstractSplitReader(RecordReader<OUT,CURSOR> recordReader, ISplit split){
//        this.recordReader = recordReader;
//        this.split = split;
//    }
//
//    protected abstract InputSplit convertInputSplit(ISplit split);
//
//    protected abstract String convertCursorToOffset(CURSOR cusor);
//
//
//    protected abstract CURSOR convertOffsetToCursor(String offset);
//
//    protected abstract List<PullMessage> convertMessageToPullMessag(OUT out);
//
//    @Override
//    public void open(ISplit split) {
//        try {
//            recordReader.open(convertInputSplit(split),createRuntimeContext());
//        } catch (IOException e) {
//            throw new RuntimeException("open reader error",e);
//        }
//    }
//
//
//    @Override
//    public boolean next() {
//        try {
//            return recordReader.next();
//        }catch (Exception e){
//            throw new RuntimeException("fetch pull data error ",e);
//        }
//
//
//    }
//
//    @Override
//    public List<PullMessage> getMessage() {
//
//        try {
//            return convertMessageToPullMessag(recordReader.getMessage());
//        }catch (Exception e){
//            throw new RuntimeException("get message error ",e);
//        }
//
//    }
//
//    @Override
//    public SplitCloseFuture close() {
//        try {
//            recordReader.close();
//        } catch (IOException e) {
//            throw new RuntimeException("close reader error ",e);
//        }
//        return new SplitCloseFuture(this,this.getSplit());
//    }
//
//    @Override
//    public void seek(String cursor) {
//        try {
//            recordReader.seek(convertOffsetToCursor(cursor));
//        } catch (IOException e) {
//            throw new RuntimeException("seek cusor error ,the offset is "+cursor,e);
//        }
//    }
//
//    @Override
//    public String getProgress() {
//        try {
//            return convertCursorToOffset(recordReader.getProgress());
//        } catch (IOException e) {
//            throw new RuntimeException("get progress error",e);
//        }
//    }
//
//    @Override
//    public long getDelay() {
//        return recordReader.getDelay();
//    }
//
//    @Override
//    public long getFetchedDelay() {
//        return recordReader.getFetchedDelay();
//    }
//
//    @Override
//    public boolean isClose() {
//        return isClose;
//    }
//
//    @Override
//    public ISplit getSplit() {
//        return split;
//    }
//
//    @Override
//    public boolean isInterrupt() {
//        return isInterrupt;
//    }
//
//    @Override
//    public boolean interrupt() {
//        isInterrupt=true;
//        return isInterrupt;
//    }
//
//    protected RuntimeContext createRuntimeContext() {
//        return new RuntimeContext() {
//            @Override
//            public String getTaskName() {
//                return split.getQueueId();
//            }
//
//            @Override
//            public MetricGroup getMetricGroup() {
//                return null;
//            }
//
//            @Override
//            public int getNumberOfParallelSubtasks() {
//                return 1;
//            }
//
//            @Override
//            public int getMaxNumberOfParallelSubtasks() {
//                return Integer.MAX_VALUE;
//            }
//
//            @Override
//            public int getIndexOfThisSubtask() {
//                return 0;
//            }
//
//            @Override
//            public int getAttemptNumber() {
//                return 1;
//            }
//
//            @Override
//            public String getTaskNameWithSubtasks() {
//                return getTaskName();
//            }
//
//            @Override
//            public ExecutionConfig getExecutionConfig() {
//                return null;
//            }
//
//            @Override
//            public ClassLoader getUserCodeClassLoader() {
//                return this.getClass().getClassLoader();
//            }
//
//            @Override
//            public <V, A extends Serializable> void addAccumulator(String s, Accumulator<V, A> accumulator) {
//
//            }
//
//            @Override
//            public <V, A extends Serializable> Accumulator<V, A> getAccumulator(String s) {
//                return null;
//            }
//
//            @Override
//            public Map<String, Accumulator<?, ?>> getAllAccumulators() {
//                return null;
//            }
//
//            @Override
//            public <V, A extends Serializable> void addPreAggregatedAccumulator(String s,
//                Accumulator<V, A> accumulator) {
//
//            }
//
//            @Override
//            public <V, A extends Serializable> Accumulator<V, A> getPreAggregatedAccumulator(String s) {
//                return null;
//            }
//
//            @Override
//            public void commitPreAggregatedAccumulator(String s) {
//
//            }
//
//            @Override
//            public <V, A extends Serializable> CompletableFuture<Accumulator<V, A>> queryPreAggregatedAccumulator(
//                String s) {
//                return null;
//            }
//
//            @Override
//            public IntCounter getIntCounter(String s) {
//                return null;
//            }
//
//            @Override
//            public LongCounter getLongCounter(String s) {
//                return null;
//            }
//
//            @Override
//            public DoubleCounter getDoubleCounter(String s) {
//                return null;
//            }
//
//            @Override
//            public Histogram getHistogram(String s) {
//                return null;
//            }
//
//            @Override
//            public boolean hasBroadcastVariable(String s) {
//                return false;
//            }
//
//            @Override
//            public <RT> List<RT> getBroadcastVariable(String s) {
//                return null;
//            }
//
//            @Override
//            public <T, C> C getBroadcastVariableWithInitializer(String s,
//                BroadcastVariableInitializer<T, C> broadcastVariableInitializer) {
//                return null;
//            }
//
//            @Override
//            public DistributedCache getDistributedCache() {
//                return null;
//            }
//
//            @Override
//            public <T> ValueState<T> getState(ValueStateDescriptor<T> valueStateDescriptor) {
//                return null;
//            }
//
//            @Override
//            public <T> ListState<T> getListState(ListStateDescriptor<T> listStateDescriptor) {
//                return null;
//            }
//
//            @Override
//            public <T> ReducingState<T> getReducingState(ReducingStateDescriptor<T> reducingStateDescriptor) {
//                return null;
//            }
//
//            @Override
//            public <IN, ACC, OUT> AggregatingState<IN, OUT> getAggregatingState(
//                AggregatingStateDescriptor<IN, ACC, OUT> aggregatingStateDescriptor) {
//                return null;
//            }
//
//            @Override
//            public <T, ACC> FoldingState<T, ACC> getFoldingState(
//                FoldingStateDescriptor<T, ACC> foldingStateDescriptor) {
//                return null;
//            }
//
//            @Override
//            public <UK, UV> MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV> mapStateDescriptor) {
//                return null;
//            }
//        };
//    }
//
//}
