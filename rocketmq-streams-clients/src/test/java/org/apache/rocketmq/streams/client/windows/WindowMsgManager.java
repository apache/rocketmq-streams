package org.apache.rocketmq.streams.client.windows;

import com.alibaba.fastjson.JSONObject;
import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.apache.rocketmq.streams.window.operator.AbstractWindow;
import org.apache.rocketmq.streams.window.operator.impl.WindowOperator;
import org.junit.Test;

public class WindowMsgManager {
    protected String fileName;
    protected Long initEventTime=1627358460609L;

    @Test
    public void testCreateMsgFiles(){
        String filePath=SingleSplitTest.class.getClassLoader().getResource(fileName).getFile();
        createFile(filePath,"");
    }


    protected void createFile(String filePath,String outFileName){
        File file=new File(filePath);
        File dir=file.getParentFile();
        String outPath=FileUtil.concatFilePath(dir.getAbsolutePath(),outFileName);
        Long time=null;
        List<String> lines= FileUtil.loadFileLine(filePath);
        List<String> msgs=new ArrayList<>();
        for(String line:lines){
            JSONObject jsonObject=JSONObject.parseObject(line);
            JSONObject msg=new JSONObject();
            msg.put("ProjectName",jsonObject.getString("ProjectName"));
            msg.put("LogStore",jsonObject.getString("LogStore"));
            msg.put("OutFlow",jsonObject.getString("OutFlow"));
            msg.put("InFlow",jsonObject.getString("InFlow"));
            if(time==null){
                time=initEventTime;
            }else {
                time=time+1;
            }
            msg.put("logTime",time);
            msg.put("currentTime", DateUtil.format(new Date(time)));

            AbstractWindow window=new WindowOperator();
            window.setSizeInterval(5);
            window.setTimeUnitAdjust(1);
            window.setTimeFieldName("logTime");
            msgs.add(msg.toJSONString());
        }
        FileUtil.write("outPath",msgs,false);

    }
}
