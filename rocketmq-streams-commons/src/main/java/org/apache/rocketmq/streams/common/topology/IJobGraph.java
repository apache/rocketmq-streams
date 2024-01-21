package org.apache.rocketmq.streams.common.topology;

import com.alibaba.fastjson.JSONObject;
import java.util.List;

public interface IJobGraph {

    void start();

    void stop();

    List<JSONObject> execute(List<JSONObject> dataList);
}
