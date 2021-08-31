package org.apache.rocketmq.streams.client.windows;

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.window.debug.WindowDebug;
import org.junit.Test;

public class WindowDebugTest {
    @Test
    public void windowDebug(){
        WindowDebug windowDebug=new WindowDebug("name1_window_10001","logTime","/tmp/rockstmq-streams","sum(total)",88121);
        windowDebug.startAnalysis();
    }

}
