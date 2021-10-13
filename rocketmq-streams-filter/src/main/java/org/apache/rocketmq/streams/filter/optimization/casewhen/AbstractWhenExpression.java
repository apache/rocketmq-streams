package org.apache.rocketmq.streams.filter.optimization.casewhen;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.streams.common.cache.compress.BitSetCache;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.NumberUtils;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.service.IScriptExpression;
import org.apache.rocketmq.streams.script.service.IScriptParamter;

public abstract class AbstractWhenExpression implements IScriptExpression {
    protected static BitSetCache cache=new BitSetCache(1000000);
    protected String namespace;
    protected String name;
    //group by varname, if has mutil varname, join by ;
    protected Map<String,GroupByVarCaseWhen> varNames2GroupByVarCaseWhen=new HashMap<>();
    //all case when element
    protected List<CaseWhenElement> allCaseWhenElement=new ArrayList<>();
    //can not cache CaseWhenElement
    protected List<CaseWhenElement> notCacheCaseWhenElement=new ArrayList<>();
    //key:varnames join by ;  value:varname list
    protected Map<String,List<String>>varNames=new HashMap<>();
    //key:varnames join by ;  value:index in allCaseWhenElement
    protected Map<String,Integer> varName2Indexs=new HashMap<>();
    //key:allCaseWhenElement value:index in allCaseWhenElement
    protected Map<CaseWhenElement,Integer> caseWhenElementIndexMap=new HashMap<>();
    //create index
    protected AtomicInteger groupIndex=new AtomicInteger(0);

    public AbstractWhenExpression(String namespace,String name){
        this.name=name;
        this.namespace=namespace;
    }

    @Override public abstract Object executeExpression(IMessage message, FunctionContext context) ;



    public void registe(CaseWhenElement caseWhenElement, Set<String> varNames){
        List<String> varNameList=new ArrayList<>();
        varNameList.addAll(varNames);
        Collections.sort(varNameList);
        String key= MapKeyUtil.createKey(varNameList);
        GroupByVarCaseWhen groupByVarCaseWhen=varNames2GroupByVarCaseWhen.get(key);
        if(groupByVarCaseWhen==null){
            groupByVarCaseWhen=new GroupByVarCaseWhen(groupIndex.incrementAndGet());
            varNames2GroupByVarCaseWhen.put(key,groupByVarCaseWhen);
        }
        groupByVarCaseWhen.registe(caseWhenElement);
        allCaseWhenElement.add(caseWhenElement);
        this.varNames.put(key,varNameList);
        this.varName2Indexs.put(key,allCaseWhenElement.size()-1);
        caseWhenElementIndexMap.put(caseWhenElement,allCaseWhenElement.size()-1);
    }

    public void compile(){
        Map<String,GroupByVarCaseWhen> groupByVarCaseWhenMap=new HashMap<>();
        for(String key:varNames2GroupByVarCaseWhen.keySet()){
            GroupByVarCaseWhen groupByVarCaseWhen=varNames2GroupByVarCaseWhen.get(key);
            if(groupByVarCaseWhen.size()<5||varNames.get(key).size()>5){
                notCacheCaseWhenElement.addAll(groupByVarCaseWhen.getAllCaseWhenElements());
            }else {
                groupByVarCaseWhenMap.put(key,groupByVarCaseWhen);
            }
            //最大个数不超过1个字节能表示的数量
            if(groupByVarCaseWhen.size()>254){
                List<CaseWhenElement> removeElements=groupByVarCaseWhen.removeUtilSize(255);
                if(removeElements!=null){
                    this.notCacheCaseWhenElement.addAll(removeElements);
                }
            }

        }
        this.varNames2GroupByVarCaseWhen=groupByVarCaseWhenMap;
    }


    @Override public List<IScriptParamter> getScriptParamters() {
        return null;
    }

    @Override public String getFunctionName() {
        return "condition";
    }
    @Override public String getExpressionDescription() {
        return null;
    }

    @Override public Object getScriptParamter(IMessage message, FunctionContext context) {
        return null;
    }

    @Override public String getScriptParameterStr() {
        return null;
    }

    @Override public List<String> getDependentFields() {
        Set<String> varNames=new HashSet<>();
        for(CaseWhenElement caseWhenElement:this.allCaseWhenElement){
            varNames.addAll(caseWhenElement.getDependentFields());
        }
        List<String> varNameList=new ArrayList<>();
        varNameList.addAll(varNames);
        Collections.sort(varNameList);
        return varNameList;
    }

    @Override public Set<String> getNewFieldNames() {
        Set<String> varNames=new HashSet<>();
        for(CaseWhenElement caseWhenElement:this.allCaseWhenElement){
            varNames.addAll(caseWhenElement.getDependentFields());
        }
        return varNames;
    }



    protected List<CaseWhenElement> executeGroupByVarCaseWhen(String key,GroupByVarCaseWhen groupByVarCaseWhen, IMessage message, FunctionContext context) {
        List<String> varList=varNames.get(key);
        String varValue=createVarValue(varList,message);
        String cacheKey=MapKeyUtil.createKey(namespace,name,groupByVarCaseWhen.index+"",varValue);
        BitSetCache.BitSet bitSet = cache.get(key);
        List<CaseWhenElement> matchCaseWhenElements=new ArrayList<>();
        if(bitSet==null){
            List<Integer> matchIndexs=groupByVarCaseWhen.executeCase(message,context,executeThenDirectly(),matchCaseWhenElements);
            bitSet=new BitSetCache.BitSet(createBytes(matchIndexs));
            cache.put(cacheKey,bitSet);
            return matchCaseWhenElements;
        }else {
            byte[] bytes=bitSet.getBytes();
            List<Integer> matchIndexs=createMatchIndex(bytes);
            groupByVarCaseWhen.executeByResult(message,context,matchIndexs,executeThenDirectly(),matchCaseWhenElements);
            return matchCaseWhenElements;
        }

    }

    protected abstract boolean executeThenDirectly();


    protected byte[] createBytes(List<Integer> indexs) {
        if(CollectionUtil.isEmpty(indexs)){
            byte[] bytes=new byte[1];
            bytes[0]=NumberUtils.toByte(0)[0];
            return bytes;
        }
        byte[] bytes=new byte[indexs.size()];
        for(int i=0;i<bytes.length;i++){
            Integer index=indexs.get(i)+1;//if index=0, maybe cannot judge is false or index
            byte b=NumberUtils.toByte(index)[0];
            bytes[i]=b;
        }
        return bytes;
    }


    protected List<Integer> createMatchIndex(byte[] bytes) {
        if(bytes.length==1&&NumberUtils.toInt(bytes)==0){
            return null;
        }
        List<Integer> matchIndexs=new ArrayList<>();
        for(byte b:bytes){
           Integer index= NumberUtils.toInt(b);
           matchIndexs.add(index-1);
        }
        return matchIndexs;
    }
    /**
     *  var name to var message value
     * @param varNames var name list
     * @param message
     * @return
     */
    protected String createVarValue(List<String> varNames, IMessage message) {
        StringBuilder stringBuilder=new StringBuilder();
        for(String varName:varNames){
            String varValue=message.getMessageBody().getString(varName);
            stringBuilder.append(varValue+";");
        }
        return stringBuilder.toString();
    }

}
