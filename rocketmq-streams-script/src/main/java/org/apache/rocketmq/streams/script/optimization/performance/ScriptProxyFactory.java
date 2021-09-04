package org.apache.rocketmq.streams.script.optimization.performance;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.streams.common.calssscaner.AbstractScan;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.script.service.IScriptExpression;

public class ScriptProxyFactory {
    protected List<AbstractScriptProxy> expressionProxies=new ArrayList<>();
    protected static ScriptProxyFactory expressionProxyFactory=new ScriptProxyFactory();
    protected static AtomicBoolean isFinishScan=new AtomicBoolean(false);
    protected AbstractScan scan=new AbstractScan() {
        @Override protected void doProcessor(Class clazz) {
            if(AbstractScriptProxy.class.isAssignableFrom(clazz)&&!Modifier.isAbstract(clazz.getModifiers())){
                AbstractScriptProxy abstractExpressionProxy=(AbstractScriptProxy)ReflectUtil.forInstance(clazz,new Class[]{IScriptExpression.class},new Object[]{null});
                expressionProxies.add(abstractExpressionProxy);
            }
        }
    };



    public static ScriptProxyFactory getInstance(){
        if(isFinishScan.compareAndSet(false,true)){
            expressionProxyFactory.scan.scanPackages("org.apache.rocketmq.streams.script.optimization.performance");
            expressionProxyFactory.scan.scanPackages("org.apache.rocketmq.streams.filter.optimization");
        }
        return expressionProxyFactory;
    }

    public AbstractScriptProxy create(IScriptExpression oriScriptExpression){
        for(AbstractScriptProxy abstractExpressionProxy: expressionProxies){
            abstractExpressionProxy.setOrigExpression(oriScriptExpression);
            if(abstractExpressionProxy.supportOptimization(oriScriptExpression)){
                return (AbstractScriptProxy)ReflectUtil.forInstance(abstractExpressionProxy.getClass(),new Class[]{IScriptExpression.class},new Object[]{oriScriptExpression});
            }
        }
        return null;
    }

}
