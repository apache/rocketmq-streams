package org.apache.rocketmq.streams.common.channel.impl.transit;

import org.apache.rocketmq.streams.common.channel.source.AbstractSource;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.topology.model.PipelineSourceJoiner;

public class TransitSource extends AbstractSource {
    protected String tableName;


    @Override
    public void addConfigurables(PipelineBuilder pipelineBuilder) {
        PipelineSourceJoiner pipelineSourceJoiner=new PipelineSourceJoiner();
        pipelineSourceJoiner.setSourcePipelineName(tableName);
        pipelineSourceJoiner.setPipelineName(pipelineBuilder.getPipelineName());;
        pipelineBuilder.addConfigurables(pipelineSourceJoiner);
        pipelineBuilder.addConfigurables(this);
    }



    @Override protected boolean startSource() {
        return true;
    }

    @Override public boolean supportNewSplitFind() {
        return false;
    }

    @Override public boolean supportRemoveSplitFind() {
        return false;
    }

    @Override public boolean supportOffsetRest() {
        return false;
    }

    @Override protected boolean isNotDataSplit(String queueId) {
        return false;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
