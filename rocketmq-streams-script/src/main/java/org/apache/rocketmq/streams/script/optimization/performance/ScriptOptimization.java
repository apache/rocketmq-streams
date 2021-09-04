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
package org.apache.rocketmq.streams.script.optimization.performance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.optimization.HyperscanRegex;
import org.apache.rocketmq.streams.common.optimization.cachefilter.ICacheFilter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.operator.expression.GroupScriptExpression;
import org.apache.rocketmq.streams.script.operator.expression.ScriptExpression;
import org.apache.rocketmq.streams.script.operator.impl.FunctionScript;
import org.apache.rocketmq.streams.script.service.IScriptExpression;

public class ScriptOptimization {
    protected String name;//function script namespace,name


    /**
     * optimizate expression
     */
    protected ScriptExpressionGroupsProxy scriptExpressionGroupsProxy =new ScriptExpressionGroupsProxy(160,1000000);

    //the optimizated script
    protected List<IScriptExpression> scriptExpressions;


    //newFieldName created in the script
    protected Map<String, IScriptExpression> newFieldName2Expressions = new HashMap<>();


    /**
     * Optimization once
     */
    protected AtomicBoolean startOptimization = new AtomicBoolean(false);

    /**
     * 目前主要是对group脚本做优化，把group中的if表达式注册到Hypersacan中做一次性快速处理。并从脚本中去除
     *
     * @param scriptExpressions
     */
    public ScriptOptimization(String name,List<IScriptExpression> scriptExpressions) {
        this.name=name;
        this.scriptExpressions = scriptExpressions;

        /**
         * get all new fieldname created in this script
         * the expression which used new fieldname can not optimize
         */
        for (IScriptExpression scriptExpression : scriptExpressions) {
            if (GroupScriptExpression.class.isInstance(scriptExpression)) {
                continue;
            }
            Set<String> newFieldNames = scriptExpression.getNewFieldNames();
            if (newFieldNames == null || newFieldNames.size() == 0) {
                continue;
            }
            String newFieldName = newFieldNames.iterator().next();
            newFieldName2Expressions.put(newFieldName, scriptExpression);
        }
    }

    /**
     * 是否支持优化
     *
     * @return
     */
    public boolean supportOptimize() {
        return false;
    }



    /**
     * 把表达式拆成3段，创建变量的，正则类，其他。正则类用HyperscanRegex做优化
     */
    public List<IScriptExpression> optimize() {
        if (!startOptimization.compareAndSet(false, true)) {
            return this.scriptExpressions;
        }
        Set<String> newVarNames=new HashSet<>();
        List<IScriptExpression> allScriptExpressions = new ArrayList<>();//最终输出的表达式列表
        List<IScriptExpression> proxyExpressions = new ArrayList<>();//最后执行的脚本，在执行完正则后执行的部分
        List<IScriptExpression> lastExpressions = new ArrayList<>();//最后执行的脚本，在执行完正则后执行的部分
        List<IScriptExpression> mapExpressions = new ArrayList<>();//如果是trim，cast，concat等函数，优先执行
        for (IScriptExpression scriptExpression : scriptExpressions) {

            Set<String> newFieldNames = scriptExpression.getNewFieldNames();
            if (newFieldNames != null&&newFieldNames.size() > 0) {
                String newFieldName = newFieldNames.iterator().next();
                newVarNames.add(newFieldName);
            }





            IScriptExpression scriptExpressionProxy = createProxy(scriptExpression,newVarNames);
            String functionName = scriptExpressionProxy.getFunctionName();
            if(scriptExpressionProxy instanceof AbstractScriptProxy){
                proxyExpressions.add(scriptExpressionProxy);
            }else if("trim".equals(functionName) || "lower".equals(functionName) || "concat".equals(functionName)){
                mapExpressions.add(scriptExpressionProxy);
            }else {
                lastExpressions.add(scriptExpressionProxy);
            }
        }
        allScriptExpressions.addAll(mapExpressions);//把优先执行的表达式添加上
        if(this.scriptExpressionGroupsProxy.scriptExpressions.size()>0){
            allScriptExpressions.add(this.scriptExpressionGroupsProxy);
        }
        allScriptExpressions.addAll(lastExpressions);//把剩余的表达式增加到list中

        this.scriptExpressions=allScriptExpressions;
        this.scriptExpressionGroupsProxy.removeLessCount();
        return this.scriptExpressions;
    }




    /**
     * 如果脚本中有较多的正则表达式，则统一注册到正则库，并行执行。
     *
     * @param scriptExpression
     * @return
     */
    protected IScriptExpression createProxy(IScriptExpression scriptExpression,Set<String> newVarNames) {
        AbstractScriptProxy scriptProxy = ScriptProxyFactory.getInstance().create(scriptExpression);
        if (scriptProxy == null) {
            return scriptExpression;
        }

        List<String> dependentFields = scriptExpression.getDependentFields();
        /**
         * 如果依赖的字段是其他脚本产生的，则不做优化
         */
        for (String fieldName : dependentFields) {
            if (newFieldName2Expressions.containsKey(fieldName)&&!newVarNames.contains(fieldName)) {
                return scriptExpression;
            }
        }

        this.scriptExpressionGroupsProxy.addScriptExpression(scriptProxy);
        List<ICacheFilter> cacheFilters=scriptProxy.getCacheFilters();
        if(cacheFilters!=null){
            for(ICacheFilter cacheFilter:cacheFilters){
                this.scriptExpressionGroupsProxy.addOptimizationExpression(this.name,cacheFilter);
            }
        }
        return scriptProxy;
    }


    public static void main(String[] args) {
        String scriptValue = "source='netstat_ob';\n"
            + "____regex_10001=regex(std_cmdline,'^(((/?([a-zA-Z0-9_\\.\\-]+/){1,20})bin/)|/bin/|/|-)?"
            + "(bash|sh|dash|ash|tcsh|csh|ksh)(\\s+[\\-a-z0-9]{1,5}){1,5}\\s*$');\n"
            + "____regex_10002=regex(std_cmdline,'-[a-z0-9]{0,4}i[a-z0-9]{0,4}');\n"
            + "____regex_10003=regex(std_cmdline,'^(((/?([a-zA-Z0-9_\\.\\-]+/){1,20})bin/)|/bin/|/|-)?zsh\\s*$');\n"
            + "____regex_10004=regex(std_cmdline,'(mkfifo|mknod).*&&\\s*(nc|telnet).*<.*\\|.*ash');\n"
            + "____equals_10002=equals(____regex_10004,true);\n"
            + "____regex_10005=regex(std_cmdline,'(mkfifo|mknod)\\s+.*?(nc|telnet)\\s+\\d{1,3}\\.\\d{1,3}\\.\\d{1,"
            + "3}\\.\\d{1,3}\\s+\\d+');\n"
            + "____equals_10003=equals(____regex_10005,true);\n"
            + "____regex_10006=regex(std_cmdline,'(mkfifo|mknod).*&&.*\\s+(nc|telnet).*<.*\\|.*ash');\n"
            + "____equals_10004=equals(____regex_10006,true);\n"
            + "____regex_10007=regex(std_cmdline,'(mkfifo|mknod).*cat.*\\|.*sh.*?-[a-z0-9]{0,4}i.*?\\|\\s*(nc|telnet)"
            + "\\s+');\n"
            + "____equals_10005=equals(____regex_10007,true);\n"
            + "____regex_10008=regex(std_cmdline,'ruby\\s+-rsocket\\s+-[a-z0-9]{0,4}e.*tcpsocket.*exec.*-[a-z0-9]{0,"
            + "4}i');\n"
            + "____equals_10006=equals(____regex_10008,true);\n"
            + "____regex_10009=regex(std_cmdline,'ruby\\s+-rsocket\\s+-[a-z0-9]{0,4}e.*tcpsocket.*popen');\n"
            + "____equals_10007=equals(____regex_10009,true);\n"
            + "____regex_10010=regex(std_cmdline,'php\\s+-[a-z0-9]{0,4}r.*fsockopen.*exec.*-[a-z0-9]{0,4}i');\n"
            + "____equals_10008=equals(____regex_10010,true);\n"
            + "____regex_10011=regex(std_cmdline,'python\\s+-[a-z0-9]{0,4}c.*exec.*import\\s+socket.*socket.*connect"
            + ".*-[a-z0-9]{0,4}i');\n"
            + "____equals_10009=equals(____regex_10011,true);\n"
            + "____regex_10012=regex(std_cmdline,'python\\s+-[a-z0-9]{0,4}c.*exec.*import\\s+base64.*base64.*decode')"
            + ";\n"
            + "____equals_10010=equals(____regex_10012,true);\n"
            + "____regex_10013=regex(std_cmdline,'python\\s+-[a-z0-9]{0,4}c\\s+(#######)?import\\s+socket.*socket\\"
            + ".socket\\(socket\\.af_inet.*\\.connect\\(.*subprocess\\.call');\n"
            + "____equals_10011=equals(____regex_10013,true);\n"
            + "____regex_10014=regex(std_cmdline,'python\\s+-[a-z0-9]{0,4}c.*exec.*import\\s+socket\\s*,"
            + "\\s*subprocess.*?connect\\(.*?open');\n"
            + "____equals_10012=equals(____regex_10014,true);\n"
            + "____regex_10015=regex(std_cmdline,'python\\s+-[a-z0-9]{0,4}c\\s+import\\s+pty\\s*;\\s*pty\\"
            + ".spawn\\s*\\(\\s*~~~~~/bin/\\w*sh~~~~~\\s*\\)');\n"
            + "____equals_10013=equals(____regex_10015,true);\n"
            + "____regex_10016=regex(std_cmdline,'python\\s+-[a-z0-9]{0,4}c.*?exec.*?base64');\n"
            + "____equals_10014=equals(____regex_10016,true);\n"
            + "____regex_10017=regex(std_cmdline,'perl.*-[a-z0-9]{0,4}e.*socket\\(.*connect.*open.*exec.*-[a-z0-9]{0,"
            + "4}i');\n"
            + "____equals_10015=equals(____regex_10017,true);\n"
            + "____regex_10018=regex(std_cmdline,'perl.*-[a-z0-9]{0,4}e.*io::socket.*fdopen.*system');\n"
            + "____equals_10016=equals(____regex_10018,true);\n"
            + "____regex_10019=regex(std_cmdline,'lua\\s+-[a-z0-9]{0,4}e[a-z0-9]{0,4}\\s+~~~~~require\\s*\\"
            + "(\\s*#######socket#######\\s*\\)\\s*;\\s*require\\s*\\(\\s*#######os#######\\s*\\)\\s*;.*?socket"
            + ".*?:connect.*?execute\\(.*?sh');\n"
            + "____equals_10017=equals(____regex_10019,true);\n"
            + "____regex_10020=regex(std_cmdline,'(ba)?sh\\s+-[a-z0-9]{0,4}i.*/dev/(tcp|udp)/\\d{1,3}\\.\\d{1,3}\\"
            + ".\\d{1,3}\\.\\d{1,3}/');\n"
            + "____equals_10018=equals(____regex_10020,true);\n"
            + "____regex_10021=regex(std_cmdline,'exec\\s+\\d+.*?/dev/(tcp|udp)/\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\"
            + ".\\d{1,3}/\\d+');\n"
            + "____equals_10019=equals(____regex_10021,true);\n"
            + "____regex_10022=regex(std_cmdline,'(^|&&|;|\\||>|>>|\\s+|/|#######)cryptcat\\s+(-[a-z]{1,4}\\s+)"
            + "*-p\\s+\\d{1,5}\\s+');\n"
            + "____equals_10020=equals(____regex_10022,true);\n"
            + "____regex_10023=regex(std_cmdline,'(^|&&|;|\\||>|>>|\\s+|/|#######)awk.*?/inet/tcp/\\d{1,5}/\\d{1,3}\\"
            + ".\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}/\\d{1,5}.*?close');\n"
            + "____equals_10021=equals(____regex_10023,true);\n"
            + "____regex_10024=regex(std_cmdline,'(^|&&|;|\\||>|>>|\\s+|/|#######)(nc|ncat)\\s+.*?-[a-z0-9]{0,"
            + "4}e[a-z0-9]{0,4}\\s+.*?/(bash|sh|dash|ash|tcsh|csh|ksh)\\s*');\n"
            + "____equals_10022=equals(____regex_10024,true);\n"
            + "____regex_10025=regex(std_cmdline,'socat\\s+exec.*?tcp:\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}');\n"
            + "____equals_10023=equals(____regex_10025,true);\n"
            + "____regex_10026=regex(std_cmdline,'zsh.*?/net/tcp.*?ztcp');\n"
            + "____equals_10024=equals(____regex_10026,true);\n"
            + "____regex_10027=regex(std_cmdline,'telnet.*?\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}.*?\\|.*?"
            + "(bash|sh|dash|ash|tcsh|csh|ksh).*?\\|.*?telnet\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}');\n"
            + "____equals_10025=equals(____regex_10027,true);\n"
            + "____regex_10028=regex(std_cmdline,'xterm\\s+-display\\s+\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\:')"
            + ";\n"
            + "____equals_10026=equals(____regex_10028,true);\n"
            + "____regex_10029=regex(std_cmdline,'awk.*?/inet/(tcp|udp)/.*?\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,"
            + "3}');\n"
            + "____equals_10027=equals(____regex_10029,true);\n"
            + "____regex_10030=regex(std_cmdline,'nc\\s+.*?spawn.*?socket.*?connect.*?\\d{1,3}\\.\\d{1,3}\\.\\d{1,"
            + "3}\\.\\d{1,3}');\n"
            + "____equals_10028=equals(____regex_10030,true);\n"
            + "____regex_10031=regex(std_cmdline,'(^|&&|;|\\||>|>>|\\s+|/|#######)(nc|ncat)\\s+.*?-[a-z0-9]{0,"
            + "3}e[a-z0-9]{0,3}\\s+.*?/(bash|sh|dash|ash|tcsh|csh|ksh)\\s*');\n"
            + "____equals_10029=equals(____regex_10031,true);\n"
            + "____regex_10032=regex(std_cmdline,'python\\s+-[a-z0-9]{0,4}c.*?base64.*?exec.*?b64decode');\n"
            + "____equals_10030=equals(____regex_10032,true);\n"
            + "____regex_10033=regex(std_cmdline,'python\\s+-[a-z0-9]{0,4}c.*?import.*?socket.*?socket\\.socket"
            + ".*?pty\\.spawn');\n"
            + "____equals_10031=equals(____regex_10033,true);\n"
            + "____regex_10034=regex(std_cmdline,'php\\s+-[a-z0-9]{0,4}r.*fsockopen.*?proc_open.*?bin/"
            + "(sh|dash|bash|zsh|csh|tcsh|ash|ksh)');\n"
            + "____equals_10032=equals(____regex_10034,true);\n"
            + "____regex_10035=regex(std_cmdline,'(^|&&|;|\\||>|>>|\\s+|/|#######)(nc|ncat)\\s+.*?-[a-z0-9]{0,"
            + "4}[ec][a-z0-9]{0,4}\\s+.*?/(bash|sh|dash|ash|tcsh|csh|ksh)\\s*');\n"
            + "____equals_10033=equals(____regex_10035,true);\n"
            + "____regex_10036=regex(std_cmdline,'(^|&&|;|\\||>|>>|\\s+|/|#######)(nc|ncat)\\s+.*?-[a-z0-9]{0,"
            + "3}[ec][a-z0-9]{0,3}\\s+.*?/(bash|sh|dash|ash|tcsh|csh|ksh)\\s*');\n"
            + "____equals_10034=equals(____regex_10036,true);\n"
            + "____regex_10037=regex(std_cmdline,'lua\\s+-{1,2}[a-z0-9]{0,4}e[a-z0-9]{0,4}\\s+.*?require.*?socket"
            + ".*?connect.*?execute.*?/(sh|dash|bash|zsh|csh|tcsh|ash|ksh)');\n"
            + "____equals_10035=equals(____regex_10037,true);\n"
            + "____regex_10038=regex(std_cmdline,'lua.*?require.*?socket.*?(tcp|send|receive)');\n"
            + "____equals_10036=equals(____regex_10038,true);\n"
            + "____regex_10039=regex(std_cmdline,'(^|\\W)ruby\\s+.*?base64.*?(system|exec|eval).*?base64.*?decode');\n"
            + "____regex_10040=regex(std_cmdline,'perl\\s+.*?-MMIME.*?(popen|exec|eval|system).*?base64');\n"
            + "____regex_10041=regex(std_cmdline,'(^|\\W)php\\s+.*?(popen|exec|eval|system).*?base64');\n"
            + "____regex_10042=regex(std_cmdline,'(^|\\W)python\\s+-c\\s+[~~~~~#######]?exec\\S+decode\\S+base64');\n"
            + "if(((____regex_10001&____regex_10002)|____regex_10003)){____case_10001='rs_1';}elseif"
            + "(____equals_10002){____case_10001='rs_2';}elseif(____equals_10003){____case_10001='rs_3';}elseif"
            + "(____equals_10004){____case_10001='rs_4';}elseif(____equals_10005){____case_10001='rs_5';}elseif"
            + "(____equals_10006){____case_10001='rs_6';}elseif(____equals_10007){____case_10001='rs_7';}elseif"
            + "(____equals_10008){____case_10001='rs_8';}elseif(____equals_10009){____case_10001='rs_9';}elseif"
            + "(____equals_10010){____case_10001='rs_10';}elseif(____equals_10011){____case_10001='rs_11';}elseif"
            + "(____equals_10012){____case_10001='rs_12';}elseif(____equals_10013){____case_10001='rs_13';}elseif"
            + "(____equals_10014){____case_10001='rs_14';}elseif(____equals_10015){____case_10001='rs_15';}elseif"
            + "(____equals_10016){____case_10001='rs_16';}elseif(____equals_10017){____case_10001='rs_17';}elseif"
            + "(____equals_10018){____case_10001='rs_18';}elseif(____equals_10019){____case_10001='rs_19';}elseif"
            + "(____equals_10020){____case_10001='rs_20';}elseif(____equals_10021){____case_10001='rs_21';}elseif"
            + "(____equals_10022){____case_10001='rs_22';}elseif(____equals_10023){____case_10001='rs_23';}elseif"
            + "(____equals_10024){____case_10001='rs_24';}elseif(____equals_10025){____case_10001='rs_25';}elseif"
            + "(____equals_10026){____case_10001='rs_26';}elseif(____equals_10027){____case_10001='rs_27';}elseif"
            + "(____equals_10028){____case_10001='rs_28';}elseif(____equals_10029){____case_10001='rs_29';}elseif"
            + "(____equals_10030){____case_10001='rs_31';}elseif(____equals_10031){____case_10001='rs_v501';}elseif"
            + "(____equals_10032){____case_10001='rs_v502';}elseif(____equals_10033){____case_10001='rs_v503';}elseif"
            + "(____equals_10034){____case_10001='rs_v504';}elseif(____equals_10035){____case_10001='rs_v505';}elseif"
            + "(____equals_10036){____case_10001='rs_v506';}elseif(____regex_10039){____case_10001='sd_01';}elseif"
            + "(____regex_10040){____case_10001='sd_02';}elseif(____regex_10041){____case_10001='sd_03';}elseif"
            + "(____regex_10042){____case_10001='sd_04';}else{____case_10001='unknown';};\n"
            + "hit_result=____case_10001;rm('____case_10001');";
        FunctionScript functionScript = new FunctionScript(scriptValue);
        functionScript.init();
    }

}
