package org.apache.rocketmq.streams.filter.optimization;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.metadata.MetaDataField;
import org.apache.rocketmq.streams.common.optimization.quicker.QuickExpression;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.model.AbstractRule;
import org.apache.rocketmq.streams.common.topology.model.AbstractStage;
import org.apache.rocketmq.streams.common.topology.stages.FilterChainStage;
import org.apache.rocketmq.streams.common.topology.stages.OutputChainStage;
import org.apache.rocketmq.streams.common.topology.stages.ScriptChainStage;
import org.apache.rocketmq.streams.common.topology.stages.UnionChainStage;
import org.apache.rocketmq.streams.common.topology.stages.WindowChainStage;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.filter.builder.ExpressionBuilder;
import org.apache.rocketmq.streams.filter.function.expression.LikeFunction;
import org.apache.rocketmq.streams.filter.function.expression.RegexFunction;
import org.apache.rocketmq.streams.filter.function.script.CaseFunction;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.script.operator.expression.GroupScriptExpression;
import org.apache.rocketmq.streams.script.operator.expression.ScriptParameter;
import org.apache.rocketmq.streams.script.operator.impl.FunctionScript;
import org.apache.rocketmq.streams.script.service.IScriptExpression;
import org.apache.rocketmq.streams.script.service.IScriptParamter;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

public class QuickExpressionExtractor {
    protected ChainPipeline chainPipeline;
    protected Integer logFingerUntilFilterIndex;// get log finger from filter of logFingerUntilFilterIndex
    protected boolean isBreaked=false;
    protected Set<String> logFingers=new HashSet<>();

    public QuickExpressionExtractor(ChainPipeline chainPipeline){
        this.chainPipeline=chainPipeline;
    }

    /**
     * parse pipeline and create QuickExpression list
     * @return
     */
    public Set<QuickExpression> parsePipeline(){
        return parsePipeline(null);
    }
    /**
     * parse pipeline and create QuickExpression list
     * @return
     */
    public Set<QuickExpression> parsePipeline(Integer filterIndex){
        MetaData metaData=chainPipeline.getChannelMetaData();
        Set<QuickExpression> result=new HashSet<>();
        List<QuickExpression> quickExpressions=new ArrayList<>();
        TopologyStage root=createTopologyStage(metaData);
        parsePipeline(chainPipeline,quickExpressions,root);
        for(QuickExpression quickExpression:quickExpressions){
            if(filterIndex==null){
                result.add(quickExpression);
                continue;
            }
            if(quickExpression.getFilterIndex()!=null&&quickExpression.getFilterIndex()>0&&quickExpression.getFilterIndex()<=filterIndex){
                result.add(quickExpression);
            }

        }
        return result;
    }

    /**
     * get logfinger from the pipeline
     * @return
     */
    public Set<String> getMsgLogfinger(){
        return this.logFingers;
    }


    /**
     * parse pipeline to create QuickExpression list
     * @param pipeline
     * @param quickExpressions regexs
     * @param parents
     * @return the pipeline last TopologyStage array
     */
    protected TopologyStage[] parsePipeline(ChainPipeline pipeline, List<QuickExpression> quickExpressions, TopologyStage... parents) {
        if(pipeline.isTopology()){
            return parseTopology(pipeline.getStageMap(),pipeline.getChannelNextStageLabel(),quickExpressions,parents);
        }else {
            List<AbstractStage> stages=pipeline.getStages();
            for(AbstractStage stage:stages){
                AtomicBoolean isBreak=new AtomicBoolean(false);
                parents= parseStage(quickExpressions,stage,isBreak,parents);
                if(parents==null){
                    break;
                }
                if(isBreak.get()){
                    break;
                }
            }
            return parents;
        }

    }

    /**
     * parse pipeline which is topology
     * @param stageMap pipeline stage map, key is stage label name ,value is stage
     * @param nextLables  next stage label name list
     * @param quickExpressions
     * @param parents
     * @return the pipeline last TopologyStage array
     */
    protected TopologyStage[] parseTopology(Map<String,AbstractStage> stageMap,List<String> nextLables,List<QuickExpression> quickExpressions,TopologyStage... parents){
        Set<TopologyStage> topologyStages=new HashSet<>();
        for(String nextLable:nextLables){
            AbstractStage stage=stageMap.get(nextLable);
            AtomicBoolean isBreak=new AtomicBoolean(false);
            TopologyStage[] topologyStageList=parseStage(quickExpressions,stage,isBreak,parents);
            if(topologyStageList==null){
                addTopologyStages(topologyStages,parents);
                continue;
            }
            List<String> children=stage.getNextStageLabels();
            if(children==null||children.size()==0){
                addTopologyStages(topologyStages,topologyStageList);
                continue;
            }

            if(isBreak.get()){
                addTopologyStages(topologyStages,topologyStageList);
                continue;
            }
            parseTopology(stageMap,children,quickExpressions,topologyStageList);
        }
        return toArray(topologyStages);
    }



    /**
     * parse stage to SourceFieldDependentLink arrays
     * @param quickExpressions
     * @param stage
     * @param parents
     * @return
     */
    protected TopologyStage[] parseStage(List<QuickExpression> quickExpressions,AbstractStage stage,AtomicBoolean isBreak, TopologyStage... parents) {
        if(stage.isAsyncNode()){
            return null;
        }
        for(TopologyStage topologyStage:parents){
            if(topologyStage.isBreak){
                return null;
            }
        }
        if(stage instanceof ScriptChainStage){
            TopologyStage topologyStage= parseScriptStage(quickExpressions,(ScriptChainStage)stage,isBreak,parents);
            topologyStage.setBreak(isBreak.get());
            if(isBreak.get()){
                isBreaked=true;
            }
            return new TopologyStage[]{topologyStage};
        }else if(stage instanceof FilterChainStage){
            TopologyStage topologyStage= parseFilterStage(quickExpressions,(FilterChainStage)stage,parents);
            return new TopologyStage[]{topologyStage};
        }else if(stage instanceof UnionChainStage){

            return parseUnionStage(quickExpressions,(UnionChainStage)stage,parents);
        }else if(stage instanceof OutputChainStage){
            return null;
        }else if(stage instanceof WindowChainStage){
            return null;
        }else {
            throw new RuntimeException("can not support parse this stage "+stage.getClass().getName());
        }
    }

    /**
     * parse script stage
     *
     * @param quickExpressions
     * @param stage
     * @param parents
     */
    protected TopologyStage parseScriptStage(List<QuickExpression> quickExpressions,ScriptChainStage stage,
        AtomicBoolean isBreak, TopologyStage... parents) {
        FunctionScript functionScript=(FunctionScript) stage.getScript();
        List<IScriptExpression> scriptExpressions=functionScript.getScriptExpressions();
        TopologyStage topologyStage=new TopologyStage(parents);
        if(scriptExpressions!=null&&scriptExpressions.size()>0){
            for(IScriptExpression scriptExpression:scriptExpressions){
                if(scriptExpression instanceof GroupScriptExpression){
                    //parse group script
                    parseGroupScriptExpression(stage,(GroupScriptExpression) scriptExpression,quickExpressions,isBreak,topologyStage);
                }else {
                    //parse not group expression
                   parseScriptExpression(stage,scriptExpression,quickExpressions,isBreak,topologyStage);
                }
            }
        }
        return topologyStage;
    }



    protected boolean isRuleExpression(String ruleExpression) {
        return ruleExpression.startsWith("'(")&&ruleExpression.endsWith(")'");
    }

    /**
     * pare filter stage
     * @param quickExpressions
     * @param stage
     * @param parents
     */
    protected TopologyStage parseFilterStage(List<QuickExpression> quickExpressions,FilterChainStage stage, TopologyStage... parents) {
        TopologyStage topologyStage=new TopologyStage(parents);


        List<AbstractRule> rules=stage.getRules();
        if(rules!=null){
            for(AbstractRule rule:rules){
                parseRule(stage,(Rule) rule,quickExpressions,topologyStage);
            }
        }
        return topologyStage;
    }

    private TopologyStage[] parseUnionStage(List<QuickExpression> quickExpressions,UnionChainStage stage, TopologyStage... parents) {
        List<ChainPipeline> pipelines=stage.getPiplines();
        List<TopologyStage> topologyStages=new ArrayList<>();
        if(pipelines!=null){
            for(ChainPipeline pipeline:pipelines){
                TopologyStage[] currentPipelineTopologyStage=  parsePipeline(pipeline,quickExpressions,parents);
                if(currentPipelineTopologyStage!=null){
                    for(TopologyStage topologyStage:currentPipelineTopologyStage){
                        topologyStages.add(topologyStage);
                    }
                }
            }
        }
        return toArray(topologyStages);
    }


    /**
     * parse GroupScriptExpression
     * @param stage
     * @param groupScriptExpression
     * @param quickExpressions
     * @param aBreak
     * @param topologyStage
     */
    protected void parseGroupScriptExpression(ScriptChainStage stage, GroupScriptExpression groupScriptExpression, List<QuickExpression> quickExpressions, AtomicBoolean aBreak, TopologyStage topologyStage) {
        List<IScriptExpression> ifExpressions=new ArrayList<>();
        ifExpressions.add(groupScriptExpression.getIfExpresssion());
        if(groupScriptExpression.getElseIfExpressions()!=null&&groupScriptExpression.getElseIfExpressions().size()>0){
            for(GroupScriptExpression expression:groupScriptExpression.getElseIfExpressions()){
                ifExpressions.add(expression.getIfExpresssion());
            }
        }
        Set<String> allSourceFieldNames=new HashSet<>();
        for(IScriptExpression ifExpression:ifExpressions){
            if(!CaseFunction.isCaseFunction(ifExpression.getFunctionName())){
                throw new RuntimeException("can not support the function parse, expect if, real is "+ifExpression.getFunctionName());
            }
            String ruleExpression=getParameterValue((IScriptParamter) ifExpression.getScriptParamters().get(0));
            Rule rule= ExpressionBuilder.createRule("tmp","tmp",ruleExpression);
            Set<String> ruleSourceFieldNames=parseRule(stage,rule,quickExpressions,topologyStage);
            if(ruleSourceFieldNames!=null){
                allSourceFieldNames.addAll(ruleSourceFieldNames);
            }
        }
        String newFieldName=groupScriptExpression.getNewFieldNames().iterator().next();
        topologyStage.putSourceNames(newFieldName,allSourceFieldNames);
    }

    /**
     * parse script expression not include group expression
     * @param scriptExpression
     * @param quickExpressions
     * @param topologyStage
     */
    private void parseScriptExpression(AbstractStage stage,IScriptExpression scriptExpression,List<QuickExpression> quickExpressions,
        AtomicBoolean isBreak,TopologyStage topologyStage) {
        if(scriptExpression.getNewFieldNames()==null||scriptExpression.getNewFieldNames().size()==0){
            return ;
        }
        String newFieldName=scriptExpression.getNewFieldNames().iterator().next();
        if(StringUtil.isEmpty(newFieldName)){
            return ;
        }
        List<String> dependentFields= scriptExpression.getDependentFields();
        if(dependentFields==null||dependentFields.size()==0){
            topologyStage.putSourceNames(newFieldName,new HashSet<>());
            return ;
        }


        //udtf can not support
        if(dependentFields.size()==1&&dependentFields.iterator().next().startsWith("UDTF")){
            isBreak.set(true);
            return;
        }
        topologyStage.addScriptExpression(scriptExpression);
        Set<String> sourceFieldNameList=new HashSet<>();
        List<IScriptExpression> depenentScripts=new ArrayList<>();
        for(String fieldName:dependentFields){
            if(isRuleExpression(fieldName)){
                Rule rule= ExpressionBuilder.createRule("tmp","tmp", FunctionUtils.getConstant(fieldName));
                Set<String> ruleSourceFieldNames=parseRule(stage,rule,quickExpressions,topologyStage);
                if(ruleSourceFieldNames!=null){
                    sourceFieldNameList.addAll(ruleSourceFieldNames);
                }
                continue;
            }
            Set<String> sourceFieldNames=traceSourceField(fieldName,topologyStage);
            if(sourceFieldNames==null){
                if(fieldName.startsWith("____null_")){//null field ，not need process
                   continue;
                }
                throw new RuntimeException("can not trace source for "+fieldName);
            }
            if(sourceFieldNames!=null){
                sourceFieldNameList.addAll(sourceFieldNames);
            }
            if(!sourceFieldNames.contains(fieldName)){
                List<IScriptExpression> scripts=new ArrayList<>();
                traceScriptList(fieldName,topologyStage,scripts);
               if(scripts!=null&&scripts.size()>0){
                   depenentScripts.addAll(scripts);
               }
            }

        }
        topologyStage.putSourceNames(newFieldName,sourceFieldNameList);
        if(org.apache.rocketmq.streams.script.function.impl.string.RegexFunction.isRegexFunction(scriptExpression.getFunctionName())){
            List<IScriptParamter> paramters=scriptExpression.getScriptParamters();
            String varName=getParameterValue(paramters.get(0));
            Set<String> sourceFieldNames=traceSourceField(varName,topologyStage);
            if(sourceFieldNames==null||sourceFieldNames.size()!=1){
                return;
            }
            String sourceFieldName=sourceFieldNames.iterator().next();
            String regex=getParameterValue(paramters.get(1));
            QuickExpression quickExpression=new QuickExpression(sourceFieldName,regex);
            if(stage.getLabel()!=null){
                String[] values=stage.getLabel().split("_");
                Integer index=Integer.valueOf(values[values.length-1])-10000;
                quickExpression.setScriptIndex(index);
            }
            if(depenentScripts!=null&&depenentScripts.size()>0){
                quickExpression.setScripts(depenentScripts);
            }

            quickExpressions.add(quickExpression);
        }
    }



    protected Set<String> parseRule(AbstractStage stage,Rule rule, List<QuickExpression> quickExpressions, TopologyStage topologyStage) {

        Collection<Expression> expressionSet = rule.getExpressionMap().values();
        if(expressionSet==null){
            return null;
        }
        Set<String> allSourceNames=new HashSet<>();
        for(Expression expression:expressionSet){

            Set<String> dependentFields=expression.getDependentFields(rule.getExpressionMap());
            Set<String> sourceFieldNameList=new HashSet<>();
            List<IScriptExpression> scriptExpressions=new ArrayList<>();
            for(String fieldName:dependentFields){
                Set<String> sourceFieldNames=traceSourceField(fieldName,topologyStage);
                if(sourceFieldNames==null){
                    throw new RuntimeException("can not trace source for "+fieldName+". the rule is "+rule.toExpressionString());
                }
                if(sourceFieldNames!=null){
                    allSourceNames.addAll(sourceFieldNames);
                    sourceFieldNameList.addAll(sourceFieldNames);
                }

                traceScriptList(fieldName,topologyStage,scriptExpressions);

            }
            Integer index=null;
            if(stage.getLabel()!=null){
                String[] values=stage.getLabel().split("_");
                index=Integer.valueOf(values[values.length-1])-10000;
                if(this.logFingerUntilFilterIndex==null){
                    this.logFingers.addAll(sourceFieldNameList);
                }
                if(this.logFingerUntilFilterIndex!=null&&index<=this.logFingerUntilFilterIndex){
                    this.logFingers.addAll(sourceFieldNameList);
                }
            }

            if(RegexFunction.isRegex(expression.getFunctionName())|| LikeFunction.isLikeFunciton(expression.getFunctionName())){
                Set<String> sourceFieldNames=traceSourceField(expression.getVarName(),topologyStage);
                if(sourceFieldNames==null||sourceFieldNames.size()!=1){
                    return allSourceNames;
                }
                String sourceFieldName=sourceFieldNames.iterator().next();
                QuickExpression quickExpression= new QuickExpression(sourceFieldName,(String)expression.getValue());
                if(index!=null){
                    quickExpression.setFilterIndex(index);
                }
                if(scriptExpressions.size()>0){
                    quickExpression.setScripts(scriptExpressions);
                }

                if(LikeFunction.isLikeFunciton(expression.getFunctionName())){
                    quickExpression.setRegex(false);
                    quickExpression.setCaseInsensitive(false);
                }
                quickExpressions.add(quickExpression);
            }
        }
        return allSourceNames;
    }


    /**
     * trace source field name by current fieldName
     * @param fieldName
     * @param topologyStage
     * @return
     */
    protected Set<String> traceSourceField(String fieldName, TopologyStage topologyStage) {
        TopologyStage current=topologyStage;
        Set<String> result= current.getSourceNames(fieldName);
        if(result!=null){
            return result;
        }
        TopologyStage[] parents=topologyStage.getParents();
        if(parents==null){
            return null;
        }
        for(TopologyStage parent:parents){
            Set<String> sourceFields=traceSourceField(fieldName,parent);
            if(sourceFields!=null){
                return sourceFields;
            }
        }
        return null;
    }


    protected void traceScriptList(String fieldName, TopologyStage stage, List<IScriptExpression> scriptExpressions) {
        TopologyStage source=stage;
        TopologyStage parent=stage.getParents()==null?null:stage.getParents()[0];
        while (parent!=null){
            source=parent;
            parent=parent.getParents()==null?null:parent.getParents()[0];
        }
        if(source.getSourceNames(fieldName)!=null){
            return;
        }
        TopologyStage current=stage;
        IScriptExpression scriptExpression= current.getScriptExpression(fieldName);
        if(scriptExpression!=null){
            scriptExpressions.add(0,scriptExpression);
            scanScriptExpressionField(scriptExpression,stage,scriptExpressions);
        }
        TopologyStage[] parents=stage.getParents();
        if(parents==null){
            return;
        }
        for(TopologyStage parentTopologyStage:parents){
            scriptExpression= parentTopologyStage.getScriptExpression(fieldName);
            if(scriptExpression!=null){
                scriptExpressions.add(scriptExpression);
                scanScriptExpressionField(scriptExpression,parentTopologyStage,scriptExpressions);
            }else {
                traceScriptList(fieldName,parentTopologyStage,scriptExpressions);
            }
        }
    }

    protected void scanScriptExpressionField(IScriptExpression expression, TopologyStage stage, List<IScriptExpression> scriptExpressions) {
        List<String> dependentFields=expression.getDependentFields();
        if(dependentFields==null||dependentFields.size()==0){
            return;
        }
        for(String fieldName:dependentFields){
            if(expression.getNewFieldNames().contains(fieldName)){
                continue;
            }
            traceScriptList(fieldName,stage,scriptExpressions);
        }
    }

    /**
     * get parameter value for rege or like
     * @param scriptParamter
     * @return
     */
    protected String getParameterValue(IScriptParamter scriptParamter) {
        if (!ScriptParameter.class.isInstance(scriptParamter)) {
            return null;
        }
        ScriptParameter parameter = (ScriptParameter) scriptParamter;
        if (parameter.getRigthVarName() != null) {
            return null;
        }
        return FunctionUtils.getConstant(parameter.getLeftVarName());
    }



    /**
     * create source field map
     * @param metaData
     * @return
     */
    protected TopologyStage createTopologyStage(MetaData metaData) {
        List<MetaDataField> metaDataFields=metaData.getMetaDataFields();
        TopologyStage topologyStage=new TopologyStage(null);
        for(MetaDataField metaDataField:metaDataFields){
            Set<String> sourceFieldNames=new HashSet<>();
            sourceFieldNames.add(metaDataField.getFieldName());
            topologyStage.putSourceNames(metaDataField.getFieldName(),sourceFieldNames);
        }
        return topologyStage;
    }

    /**
     * add all list to stages
     * @param stages
     * @param list
     */
    protected void addTopologyStages(Set<TopologyStage> stages, TopologyStage[] list) {
        if(list==null){
            return;
        }
        for(TopologyStage stage:list){
            stages.add(stage);
        }
    }


    private TopologyStage[] toArray(Collection<TopologyStage> stages) {
        if(stages==null){
            return null;
        }
        TopologyStage[] topologyStages=new TopologyStage[stages.size()];
        int i=0;
        for(TopologyStage topologyStage:stages){
            topologyStages[i]=topologyStage;
            i++;
        }
        return topologyStages;
    }
    /**
     * map stage of pipeline
     */
    class TopologyStage{
        /**
         * key: new fieldName; value: source fieldName
         */
        private Map<String,Set<String>> sourceFieldMap=new HashMap<>();
        /**
         * key: new fieldName;value : the script list which elt from source field
         */
        private Map<String,List<String>> fieldName2ScriptList=new HashMap<>();

        /**
         * newFieldName map script
         */
        private Map<String,IScriptExpression> fieldName2ScriptExpressions=new HashMap<>();

        private TopologyStage[] parents;

        protected boolean isBreak=false;

        public TopologyStage(TopologyStage[] parents){
            this.parents=parents;
        }

        public void putSourceNames(String newFieldName,Set<String> sourceFieldName){
            this.sourceFieldMap.put(newFieldName,sourceFieldName);
        }

        public Set<String> getSourceNames(String newFieldName){
            return this.sourceFieldMap.get(newFieldName);
        }

        public void putScripts(String fieldName,List<String> scripts){
            fieldName2ScriptList.put(fieldName,scripts);
        }


        public List<String> getScriptList(String fieldName){
            return fieldName2ScriptList.get(fieldName);
        }

        public TopologyStage[] getParents() {
            return parents;
        }

        public void addScriptExpression(IScriptExpression scriptExpression){
            String newFieldName=scriptExpression.getNewFieldNames().iterator().next();
            this.fieldName2ScriptExpressions.put(newFieldName,scriptExpression);
        }

        protected IScriptExpression getScriptExpression(String newFieldName){
           return this.fieldName2ScriptExpressions.get(newFieldName);
        }

        public boolean isBreak() {
            return isBreak;
        }

        public void setBreak(boolean aBreak) {
            isBreak = aBreak;
        }
    }

    public Integer getLogFingerUntilFilterIndex() {
        return logFingerUntilFilterIndex;
    }

    public boolean isBreaked() {
        return isBreaked;
    }

    public void setLogFingerUntilFilterIndex(Integer logFingerUntilFilterIndex) {
        this.logFingerUntilFilterIndex = logFingerUntilFilterIndex;
    }
}
