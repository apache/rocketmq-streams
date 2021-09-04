package org.apache.rocketmq.streams.filter.optimization;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.streams.common.calssscaner.AbstractScan;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.filter.operator.expression.SimpleExpression;
import org.apache.rocketmq.streams.script.optimization.performance.AbstractScriptProxy;
import org.apache.rocketmq.streams.script.service.IScriptExpression;

public class ExpressionProxyFactory {
    protected List<AbstractExpressionProxy> expressionProxies = new ArrayList<>();
    protected static ExpressionProxyFactory expressionProxyFactory=new ExpressionProxyFactory();
    protected static AtomicBoolean isFinishScan = new AtomicBoolean(false);
    protected AbstractScan scan = new AbstractScan() {
        @Override protected void doProcessor(Class clazz) {
            if (AbstractExpressionProxy.class.isAssignableFrom(clazz)&& !Modifier.isAbstract(clazz.getModifiers())) {
                AbstractExpressionProxy abstractExpressionProxy = (AbstractExpressionProxy) ReflectUtil.forInstance(clazz, new Class[] {Expression.class, Rule.class}, new Object[] {null,null});
                expressionProxies.add(abstractExpressionProxy);
            }
        }
    };

    public static ExpressionProxyFactory getInstance() {
        if (isFinishScan.compareAndSet(false, true)) {
            expressionProxyFactory.scan.scanPackages("org.apache.rocketmq.streams.script.optimization");
            expressionProxyFactory.scan.scanPackages("org.apache.rocketmq.streams.filter.optimization");
        }
        return expressionProxyFactory;
    }

    public AbstractExpressionProxy create(Expression expression,Rule rule) {
        for (AbstractExpressionProxy abstractExpressionProxy : expressionProxies) {
            if (abstractExpressionProxy.support(expression)) {
                return (AbstractExpressionProxy) ReflectUtil.forInstance(abstractExpressionProxy.getClass(), new Class[] {Expression.class, Rule.class}, new Object[] {expression,rule});
            }
        }
        return null;
    }
}