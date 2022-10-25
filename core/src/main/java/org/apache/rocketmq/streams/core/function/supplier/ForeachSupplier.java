package org.apache.rocketmq.streams.core.function.supplier;

import org.apache.rocketmq.streams.core.function.ForeachAction;
import org.apache.rocketmq.streams.core.metadata.Data;
import org.apache.rocketmq.streams.core.running.AbstractProcessor;
import org.apache.rocketmq.streams.core.running.Processor;

import java.util.function.Supplier;

public class ForeachSupplier<T> implements Supplier<Processor<T>> {
    private ForeachAction<T> foreachAction;

    public ForeachSupplier(ForeachAction<T> foreachAction) {
        this.foreachAction = foreachAction;
    }

    @Override
    public Processor<T> get() {
        return new ForeachProcessor(this.foreachAction);
    }

    class ForeachProcessor extends AbstractProcessor<T> {
        private ForeachAction<T> foreachAction;

        public ForeachProcessor(ForeachAction<T> foreachAction) {
            this.foreachAction = foreachAction;
        }

        @Override
        public void process(T data) throws Throwable {
            this.foreachAction.apply(data);
            this.context.forward(this.context.getData());
        }
    }


}
