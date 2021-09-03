package org.apache.rocketmq.streams.client;

import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.apache.rocketmq.streams.script.operator.impl.FunctionScript;
import org.junit.Test;

public class ScriptOptimizationTest {

    @Test
    public void testScriptOptimization(){
        String scriptValue= FileUtil.loadFileContent("/Users/yuanxiaodong/Downloads/script.txt");
        FunctionScript functionScript=new FunctionScript(scriptValue);
        functionScript.init();

    }
}
