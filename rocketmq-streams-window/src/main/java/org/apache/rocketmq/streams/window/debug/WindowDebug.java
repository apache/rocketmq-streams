package org.apache.rocketmq.streams.window.debug;

import org.apache.rocketmq.streams.window.operator.AbstractWindow;

public class WindowDebug {

    protected String sumFieldName;
    protected int expectValue;
    protected String dir;

    protected DebugWriter debugWriter;
    protected DebugAnalysis debugAnalysis;


    public WindowDebug(String windowName,String timeFieldName,String dir,String sumFieldName, int expectValue){
        this.dir=dir;
        this.sumFieldName=sumFieldName;
        this.expectValue=expectValue;
        this.debugAnalysis=new DebugAnalysis(dir,sumFieldName,expectValue,timeFieldName);
        debugWriter=DebugWriter.getDebugWriter(windowName);
        debugWriter.setFilePath(dir);
        debugWriter.setCountFileName("total");
        debugWriter.setOpenDebug(true);
    }


    public void startAnalysis(){
        debugAnalysis.debugAnalysis();
    }


}
