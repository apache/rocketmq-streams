package org.apache.rocketmq.streams.common.optimization.cachefilter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.streams.common.calssscaner.AbstractScan;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;

public class CacheFilterFactory {

    protected List<ICacheFilterBulider> cacheFilterCreators=new ArrayList<>();
    protected static CacheFilterFactory expressionFactory=new CacheFilterFactory();
    protected static AtomicBoolean isScaned=new AtomicBoolean(false);
    protected AbstractScan scan=new AbstractScan() {
        @Override protected void doProcessor(Class clazz) {
            if(ICacheFilterBulider.class.isAssignableFrom(clazz)){
                cacheFilterCreators.add(ReflectUtil.forInstance(clazz));
            }
        }
    };

    public static CacheFilterFactory getInstance(){
        if(isScaned.compareAndSet(false,true)){
            expressionFactory.scan.scanPackages("org.apache.rocketmq.streams.script.optimization.performance");
            expressionFactory.scan.scanPackages("org.apache.rocketmq.streams.filter.function");
        }
        return expressionFactory;
    }

    public boolean supportProxy(Object oriExpression){
        for(ICacheFilterBulider cacheFilterBulider:this.cacheFilterCreators){
            if(cacheFilterBulider.support(oriExpression)){
                return true;
            }
        }
        return false;
    }

    public ICacheFilter create(Object oriExpression){
        for(ICacheFilterBulider cacheFilterBulider:this.cacheFilterCreators){
            if(cacheFilterBulider.support(oriExpression)){
                return cacheFilterBulider.create(oriExpression);
            }
        }
        return null;
    }
}
