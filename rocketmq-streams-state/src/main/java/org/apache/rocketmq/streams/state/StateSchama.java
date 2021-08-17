package org.apache.rocketmq.streams.state;

import org.apache.rocketmq.streams.common.utils.MapKeyUtil;

public class StateSchama {
    protected Object[] schemaElements;
    protected transient Class valueClass;
    public StateSchama(Class valueClass,Object... schemaElements){
        this.schemaElements=schemaElements;
        this.valueClass=valueClass;
    }


    public String getSchema(){
        String[] values=new String[schemaElements.length];
        for(int i=0;i<values.length;i++){
            values[i]=schemaElements[i]==null?"":schemaElements[i].toString();
        }
        return MapKeyUtil.createKey(values);
    }

    public Class getValueClass() {
        return valueClass;
    }
}
