package org.apache.rocketmq.streams.common.model;

public class NameCreatorContext {
    protected static ThreadLocal<NameCreator> threadLocal = new ThreadLocal<>();

    private NameCreatorContext(NameCreator nameCreator) {
        threadLocal.set(nameCreator);
    }

    public static NameCreator get() {

        NameCreator nameCreator= threadLocal.get();
        if(nameCreator==null){
            nameCreator=new NameCreator();
            threadLocal.set(nameCreator);
        }
        return nameCreator;
    }

    public static void remove() {
        threadLocal.remove();
    }

    public static NameCreatorContext init(NameCreator nameCreator) {
        return new NameCreatorContext(nameCreator);
    }

}
