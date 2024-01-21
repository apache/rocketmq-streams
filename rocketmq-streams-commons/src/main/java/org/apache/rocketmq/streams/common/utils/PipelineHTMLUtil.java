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
package org.apache.rocketmq.streams.common.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import java.util.List;
import org.apache.rocketmq.streams.common.channel.source.AbstractSource;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.topology.metric.StageGroup;
import org.apache.rocketmq.streams.common.topology.model.AbstractChainStage;
import org.apache.rocketmq.streams.common.topology.model.AbstractStage;
import org.apache.rocketmq.streams.common.topology.model.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.stages.FilterChainStage;

public class PipelineHTMLUtil {
    public static String createHTML(ChainPipeline pipeline) {
        try {

        } catch (Exception e) {
            e.printStackTrace();
        }
        StringBuilder html = new StringBuilder("<html>\n" +
            "\t<head>\n" +
            "\t\t<meta charset=\"utf-8\">\n" +
            "\t\t<title>Test</title>\n" +
            "\t\t<script type=\"text/javascript\">\n" +
            "\t\t\t\n" +
            "\t\t\tfunction toggle(number){\n" +
            "\t\t\t\tvar menu = document.getElementById(\"menu_\"+number);\n" +
            "\t\t\t\tmenu.classList.toggle('hide');\n" +
            "\t\t\t}\n" +
            "\t\t</script>\n" +
            "\t\t<style type=\"text/css\">\n" +
            "\t\t\t.hide{\n" +
            "\t\t\t\tdisplay: none;\n" +
            "\t\t\t}\n" +
            "\t\t\tli{\n" +
            "\t\t\t\tlist-style-type:none;\n" +
            "\t\t\t}\n" +
            "\t\t</style>\n" +
            "\t</head>\n" +
            "\t<body>");

        List<StageGroup> stageGroups = pipeline.getRootStageGroups();
        int i = 100;
        StringBuilder hideHtmlBuilder = new StringBuilder();
        String baseTab = "\t\t";
        html.append(baseTab + "<ol>\n");
        html.append(baseTab + "\tCREATE TABLE " + pipeline.getMsgSourceName() + "\n");

        html.append(baseTab + "\t<button onclick=toggle(" + i + ")>\n");
        html.append(baseTab + "\t\t SQL\n");
        html.append(baseTab + "\t</button>\n");
        hideHtmlBuilder.append(baseTab + "\t<li class=\"hide\" style=\"white-space: pre-line;\" id=\"menu_" + i + "\">\n");
        hideHtmlBuilder.append(baseTab + "\t\t" + pipeline.getCreateTableSQL() + "\n");
        hideHtmlBuilder.append(baseTab + "\t</li>\n");
        i++;

        html.append(baseTab + "\t<button onclick=toggle(" + i + ")>\n");
        html.append(baseTab + "\t\t 数据源信息\n");
        html.append(baseTab + "\t</button>\n");
        hideHtmlBuilder.append(baseTab + "\t<li class=\"hide\" style=\"white-space: pre-line;\" id=\"menu_" + i + "\">\n");
        if (pipeline == null || pipeline.getSource() == null) {
            return "";
        }
        JSONObject jsonObject = JSON.parseObject(pipeline.getSource().toJson());
        jsonObject.remove("metaData");
        jsonObject.remove("context");
        hideHtmlBuilder.append(baseTab + "\t\t" + JsonableUtil.formatJson(jsonObject) + "\n");
        hideHtmlBuilder.append(baseTab + "\t</li>\n");

        if (pipeline.getSource() instanceof AbstractSource) {
            AbstractSource abstractSource = (AbstractSource) pipeline.getSource();
            i++;

            html.append(baseTab + "\t<button onclick=toggle(" + i + ")>\n");
            html.append(baseTab + "\t\t 启动时间\n");
            html.append(baseTab + "\t</button>\n");
            hideHtmlBuilder.append(baseTab + "\t<li class=\"hide\" style=\"white-space: pre-line;\" id=\"menu_" + i + "\">\n");
            if (pipeline == null || pipeline.getSource() == null) {
                return "";
            }

            hideHtmlBuilder.append(baseTab + "\t\t启动时间：" + abstractSource.getSourceMetric().startTime() + "\n");
            hideHtmlBuilder.append(baseTab + "\t</li>\n");

            i++;

            html.append(baseTab + "\t<button onclick=toggle(" + i + ")>\n");
            html.append(baseTab + "\t\t 第一条消息时间\n");
            html.append(baseTab + "\t</button>\n");
            hideHtmlBuilder.append(baseTab + "\t<li class=\"hide\" style=\"white-space: pre-line;\" id=\"menu_" + i + "\">\n");
            if (pipeline == null || pipeline.getSource() == null) {
                return "";
            }

            hideHtmlBuilder.append(baseTab + "\t\t第一条消息时间：" + abstractSource.getSourceMetric().firstMsgReceive() + "\n");
            hideHtmlBuilder.append(baseTab + "\t</li>\n");

            i++;

            html.append(baseTab + "\t<button onclick=toggle(" + i + ")>\n");
            html.append(baseTab + "\t\t 最新消息时间\n");
            html.append(baseTab + "\t</button>\n");
            hideHtmlBuilder.append(baseTab + "\t<li class=\"hide\" style=\"white-space: pre-line;\" id=\"menu_" + i + "\">\n");
            if (pipeline == null || pipeline.getSource() == null) {
                return "";
            }

            hideHtmlBuilder.append(baseTab + "\t\t最新消息时间：" + abstractSource.getSourceMetric().lastMsgReceive() + "\n");
            hideHtmlBuilder.append(baseTab + "\t</li>\n");

            i++;

            html.append(baseTab + "\t<button onclick=toggle(" + i + ")>\n");
            html.append(baseTab + "\t\t 消费进度\n");
            html.append(baseTab + "\t</button>\n");
            hideHtmlBuilder.append(baseTab + "\t<li class=\"hide\" style=\"white-space: pre-line;\" id=\"menu_" + i + "\">\n");
            if (pipeline == null || pipeline.getSource() == null) {
                return "";
            }

            hideHtmlBuilder.append(baseTab + "\t\t" + abstractSource.getSourceMetric().toProgress() + "\n");
            hideHtmlBuilder.append(baseTab + "\t</li>\n");

            i++;

            html.append(baseTab + "\t<button onclick=toggle(" + i + ")>\n");
            html.append(baseTab + "\t\t 性能监控\n");
            html.append(baseTab + "\t</button>\n");
            hideHtmlBuilder.append(baseTab + "\t<li class=\"hide\" style=\"white-space: pre-line;\" id=\"menu_" + i + "\">\n");
            if (pipeline == null || pipeline.getSource() == null) {
                return "";
            }

            hideHtmlBuilder.append(baseTab + "\t\t" + abstractSource.getSourceMetric().toCost() + "\n");
            hideHtmlBuilder.append(baseTab + "\t</li>\n");
        }

        html.append(hideHtmlBuilder.toString());
        html.append(baseTab + "\n</ol>\n");
        i = 1000;
        for (StageGroup stageGroup : stageGroups) {
            String stageGroupHtml = createStageGroupHTML(stageGroup, i, 2);
            html.append(stageGroupHtml);
            i = i + 1000;
        }
        html.append("\t</body>\n");
        html.append("</html>\n");
        return html.toString();
    }

    private static String createStageGroupHTML(StageGroup group, int index, int tabSize) {
        StringBuilder stringBuilder = new StringBuilder();
        StringBuilder hideHtmlBuilder = new StringBuilder();
        String baseTab = "";
        for (int i = 0; i < tabSize; i++) {
            baseTab += "\t";
        }
        group.calculateMetric();
        stringBuilder.append(baseTab + "<ol>\n");
        stringBuilder.append(baseTab + "\t" + group.getViewName() + "\n");

//        if(isSourceStageGroup){
//            stringBuilder.append(baseTab + "\t<button onclick=toggle(" + index + ")>\n");
//            stringBuilder.append(baseTab + "\t\t SQL\n");
//            stringBuilder.append(baseTab + "\t</button>\n");
//            hideHtmlBuilder.append(baseTab + "\t<li class=\"hide\" style=\"white-space: pre-line;\" id=\"menu_" + index + "\">\n");
//            hideHtmlBuilder.append(baseTab + "\t\t" + group.getSql() + "\n");
//            hideHtmlBuilder.append(baseTab + "\t</li>\n");
//            index++;
//        }

        if (group.getChildren().size() > 0) {
            stringBuilder.append(baseTab + "\t<button onclick=toggle(" + index + ")>\n");
            stringBuilder.append(baseTab + "\t\t 展开" + group.getChildren().size() + "个子查询\n");
            stringBuilder.append(baseTab + "\t</button>\n");

            hideHtmlBuilder.append(baseTab + "\t<li class=\"hide\" id=\"menu_" + index + "\">\n");
            int i = 0;
            for (StageGroup subStageGroup : group.getChildren()) {
                subStageGroup.calculateMetric();
                int subIndex = index * 1000 + (i * 100);
                hideHtmlBuilder.append(createStageGroupHTML(subStageGroup, subIndex, tabSize + 2));
                i++;
            }
            hideHtmlBuilder.append(baseTab + "\t</li>\n");
            index++;
        }

        if (group.getAllStages().size() > 0) {
            stringBuilder.append(baseTab + "\t<button onclick=toggle(" + index + ")>\n");
            stringBuilder.append(baseTab + "\t\t 展开" + group.getAllStages().size() + "个Stage\n");
            stringBuilder.append(baseTab + "\t</button>\n");

            hideHtmlBuilder.append(baseTab + "\t<li class=\"hide\" id=\"menu_" + index + "\">\n");
            int i = 0;
            for (AbstractStage stage : group.getAllStages()) {
                int subIndex = index * 1000 + (i * 100);
                hideHtmlBuilder.append(createStageHTML(stage, subIndex, tabSize + 2));
                i++;
            }
            hideHtmlBuilder.append(baseTab + "\t</li>\n");
            index++;

        }

        stringBuilder.append(baseTab + "\t<button onclick=toggle(" + index + ")>\n");
        stringBuilder.append(baseTab + "\t\t IN FLOW: " + group.getInCount() + "\n");
        stringBuilder.append(baseTab + "\t</button>\n");
        index++;

        stringBuilder.append(baseTab + "\t<button onclick=toggle(" + index + ")>\n");
        stringBuilder.append(baseTab + "\t\t OUT FLOW: " + group.getOutCount() + "\n");
        stringBuilder.append(baseTab + "\t</button>\n");
        index++;

        stringBuilder.append(baseTab + "\t<button onclick=toggle(" + index + ")>\n");
        stringBuilder.append(baseTab + "\t\t QPS: " + group.getQps() + "\n");
        stringBuilder.append(baseTab + "\t</button>\n");
        index++;

        index++;

        stringBuilder.append(hideHtmlBuilder.toString());
        stringBuilder.append(baseTab + "\n</ol>\n");
        return stringBuilder.toString();
    }

    protected static String createStageHTML(AbstractStage stage, int index, int tabSize) {
        StringBuilder stringBuilder = new StringBuilder();
        StringBuilder hideHtmlBuilder = new StringBuilder();
        String baseTab = "";
        for (int i = 0; i < tabSize; i++) {
            baseTab += "\t";
        }

        stringBuilder.append(baseTab + "<ol>\n");

//        stringBuilder.append(baseTab + "\t<button onclick=toggle(" + index + ")>\n");
//        stringBuilder.append(baseTab + "\t\t SQL\n");
//        stringBuilder.append(baseTab + "\t</button>\n");
//        hideHtmlBuilder.append(baseTab + "\t<li class=\"hide\" style=\"white-space: pre-line;\" id=\"menu_" + index + "\">\n");
//        hideHtmlBuilder.append(baseTab + "\t\t" + stage.getSql() + "\n\n");
//        hideHtmlBuilder.append(baseTab + "\t</li>\n");
//        index++;
        if ("org.apache.rocketmq.streams.stage.ShuffleSourceChainStage".equals(stage.getClass().getName())) {

            ISource source = ReflectUtil.getDeclaredField(stage, "shuffleSource");
            // stringBuilder.append(baseTab + "\tShufflerSource:" + stage.getLabel() + "\n");
            stringBuilder.append(baseTab + "\t<button onclick=toggle(" + index + ")>\n");
            stringBuilder.append(baseTab + "\t\t Shuffle_Receive \n");
            stringBuilder.append(baseTab + "\t</button>\n");
            hideHtmlBuilder.append(baseTab + "\t<li class=\"hide\" style=\"white-space: pre-line;\" id=\"menu_" + index + "\">\n");
            JSONObject jsonObject = JSON.parseObject(source.toJson());
            jsonObject.remove("metaData");
            hideHtmlBuilder.append(baseTab + "\t\t" + JsonableUtil.formatJson(jsonObject) + "\n");
            hideHtmlBuilder.append(baseTab + "\t</li>\n");
            index++;
        } else {
//            if(OutputChainStage.class.isInstance(stage)){
//                stringBuilder.append(baseTab + "\tInsert:" + stage.getLabel() + "\n");
//            }else {
//                stringBuilder.append(baseTab + "\t" + stage.getClass().getSimpleName().replace("ChainStage", "") + ":" + stage.getLabel() + "\n");
//            }

            stringBuilder.append(baseTab + "\t<button onclick=toggle(" + index + ")>\n");
            if (AbstractChainStage.class.isInstance(stage)) {
                AbstractChainStage abstractStage = (AbstractChainStage) stage;
                stringBuilder.append(baseTab + "\t\t " + abstractStage.getDiscription() + " \n");
            } else {
                stringBuilder.append(baseTab + "\t\t  \n");
            }
            stringBuilder.append(baseTab + "\t</button>\n");

            hideHtmlBuilder.append(baseTab + "\t\t<li class=\"hide\" style=\"white-space: pre-line;\" id=\"menu_" + index + "\">\n");
            hideHtmlBuilder.append(baseTab + "\t\t\t" + stage.getSql() + "\n\n");
            hideHtmlBuilder.append(baseTab + "\t\t</li>\n");
            index++;

        }

        if ("org.apache.rocketmq.streams.stage.ShuffleSourceChainStage".equals(stage.getClass().getName())) {
            stringBuilder.append(baseTab + "\t<button>\n");
            stringBuilder.append(baseTab + "\t\t OUT FLOW: " + stage.getStageMetric().getInCount() + "\n");
            stringBuilder.append(baseTab + "\t</button>\n");
            index++;
        } else {
            stringBuilder.append(baseTab + "\t<button>\n");
            stringBuilder.append(baseTab + "\t\t IN FLOW: " + stage.getStageMetric().getInCount() + "\n");
            stringBuilder.append(baseTab + "\t</button>\n");
            index++;
        }

        if (!"org.apache.rocketmq.streams.stage.ShuffleOutputChainStage".equals(stage.getClass().getName()) && !"org.apache.rocketmq.streams.stage.ShuffleSourceChainStage".equals(stage.getClass().getName())) {
            stringBuilder.append(baseTab + "\t<button>\n");
            stringBuilder.append(baseTab + "\t\t OUT FLOW: " + stage.getStageMetric().getOutCount() + "\n");
            stringBuilder.append(baseTab + "\t</button>\n");
            index++;
        }

        if (!"org.apache.rocketmq.streams.stage.ShuffleSourceChainStage".equals(stage.getClass().getName())) {

            stringBuilder.append(baseTab + "\t<button >\n");
            stringBuilder.append(baseTab + "\t\t AVG COST TIME: " + stage.getStageMetric().getAvgCostTime() + "\n");
            stringBuilder.append(baseTab + "\t</button>\n");
            index++;

            stringBuilder.append(baseTab + "\t<button>\n");
            stringBuilder.append(baseTab + "\t\t QPS: " + stage.getStageMetric().getQps() + "\n");
            stringBuilder.append(baseTab + "\t</button>\n");
            index++;

            stringBuilder.append(baseTab + "\t<button>\n");
            stringBuilder.append(baseTab + "\t\t Max Cost: " + stage.getStageMetric().getMaxCostTime() + "\n");
            stringBuilder.append(baseTab + "\t</button>\n");
            index++;

        }

        if (FilterChainStage.class.isInstance(stage) && (stage.getStageMetric().getInCount() > stage.getStageMetric().getOutCount())) {
            FilterChainStage filterChainStage = (FilterChainStage) stage;
            stringBuilder.append(baseTab + "\t<button onclick=toggle(" + index + ")>\n");
            stringBuilder.append(baseTab + "\t\tFilter Reason\n");
            stringBuilder.append(baseTab + "\t</button>\n");

            hideHtmlBuilder.append(baseTab + "\t<li class=\"hide\" style=\"white-space: pre-line;\" id=\"menu_" + index + "\">\n");
            hideHtmlBuilder.append(baseTab + "\t\t" + filterChainStage.getStageMetric().createNotFireReason() + "\n\n");
            hideHtmlBuilder.append(baseTab + "\t</li>\n");

            index++;
        }

        if (CollectionUtil.isNotEmpty(stage.getNextStageLabels())) {
            stringBuilder.append(baseTab + "\t<button>\n");
            stringBuilder.append(baseTab + "\t\t NextStage: " + MapKeyUtil.createKey(",", stage.getNextStageLabels()) + "\n");
            stringBuilder.append(baseTab + "\t</button>\n");
            index++;
        }

        stringBuilder.append(hideHtmlBuilder.toString());
        stringBuilder.append(baseTab + "\n</ol>\n");
        return stringBuilder.toString();

    }
}
