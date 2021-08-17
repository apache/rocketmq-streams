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
package org.apache.rocketmq.streams.common.monitor;

import com.alibaba.fastjson.JSONObject;
import java.util.List;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.monitor.impl.DipperMonitor;
import org.apache.rocketmq.streams.common.monitor.impl.NothingMontior;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;

/**
 * name 的推荐用法，是层级关系，如pipline.name.channel.name/pipline.name.stage.name/channel.name.rule.name/channel.name.rule.name.expression.name
 */
public interface IMonitor {
    String MONTIOR_NAME = "monitor_name";
    String MONITOR_SUCCESS = "monitor_success";//监控中是否有错误
    String MONITOR_COST = "monitor_cost";//花费时间
    String MONTIOR_SLOW = "monitor_slow";//是否是慢查询
    String MONITOR_ERROR_MSG = "monitor_error_msgs";//如果发生错误，具体的错误信息
    String MONITOR_CONTEXT_MSG = "monitor_context_msgs";//监控对应的上下文
    String MONITOR_CHILDREN = "monitor_children";//监控对应的上下文
    String MONITOR_SAMPLE_DATA = "monitor_sample_data";//采样数据
    String MONITOR_RESULT = "monitor_result";//本次执行的结果
    IMonitor NOTHING_MONITOR = new NothingMontior();

    String TYPE_STARTUP = "startup";
    String TYPE_HEARTBEAT = "heartbeat";
    String TYPE_DATAPROCESS = "dataprocess";

    /**
     * 给一个configuranle对象创建和一个monitor
     *
     * @param configurable
     * @return
     */
    static IMonitor createMonitor(IConfigurable configurable) {
        String name = MapKeyUtil.createKeyBySign(".", configurable.getType(), configurable.getNameSpace(), configurable.getConfigureName());
        IMonitor monitor = new DipperMonitor();
        monitor.startMonitor(name);
        return monitor;
    }

    /**
     * 创建一个子项目
     *
     * @param childrenName
     * @return
     */
    IMonitor createChildren(String... childrenName);

    IMonitor createChildren(IConfigurable configurable);

    /**
     * 开始一项监控，name必须是唯一健值
     *
     * @param name 只要不重复即可，用于区分业务
     * @return
     */
    IMonitor startMonitor(String name);

    /**
     * 完成一次监控
     *
     * @return
     */
    IMonitor endMonitor();

    /**
     * 是否是一次慢操作，根据timeout来判断
     *
     * @return
     */
    boolean isSlow();

    /**
     * 本次监控是否发生错误
     *
     * @return
     */
    boolean isError();

    /**
     * 发生了错误，把错误信息记录下来
     *
     * @param e        具体的错误对象
     * @param messages 和错误相关的信息，可以多个
     * @return
     */
    IMonitor occureError(Exception e, String... messages);

    /**
     * 对于本次监控，增加一些上下文附加项目，用户可以基于附加项，做问题排查
     *
     * @param value 用户定义
     * @return
     */
    IMonitor addContextMessage(Object value);

    IMonitor setResult(Object value);

    /**
     * 增加一些采样数据
     *
     * @param context
     * @return
     */
    JSONObject setSampleData(AbstractContext context);

    /**
     * 把监控信息报告出来
     *
     * @return
     */
    JSONObject report(String level);

    /**
     * 把结果输出到给定到channel中，如果为空，则输出到日志
     */
    void output();

    /**
     * 获取所有到子monitor
     *
     * @return
     */
    List<IMonitor> getChildren();

    /**
     * 获得执行时间
     *
     * @return
     */
    public long getCost();

    /**
     * 获得名字
     *
     * @return
     */
    public String getName();

    /**
     * 类型
     *
     * @return
     */
    public String getType();

    /**
     * 设置类型
     */
    public void setType(String type);

}
