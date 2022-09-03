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
package org.apache.rocketmq.streams.common.context;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;

/**
 * 保存消息的offset，支持消息拆分后，多级offset
 */
public class MessageOffset {
    /**
     * 因为是字符串比较，需要有一个固定位数
     */
    public static final int LAYER_OFFST_INIT = 10000000;
    public static String SPLIT_SIGN = ".";

    protected String mainOffset;//数据源发送出来的offset。
    protected List<Long> offsetLayers = new ArrayList<>();//多级offset
    protected boolean isLongOfMainOffset = true;//主offset是否是long型

    public MessageOffset(String offset, boolean isLongOfMainOffset) {
        mainOffset = parseOffset(offset, offsetLayers);
        this.isLongOfMainOffset = isLongOfMainOffset;
    }

    public MessageOffset(Long mainOffset) {
        this.mainOffset = mainOffset + "";
        this.isLongOfMainOffset = true;
    }

    public MessageOffset(String mainOffset) {
        this.mainOffset = mainOffset;
        this.isLongOfMainOffset = false;
    }

    public MessageOffset(Integer mainOffset) {
        this.mainOffset = mainOffset + "";
        this.isLongOfMainOffset = true;
    }

    public MessageOffset() {
        this.mainOffset = System.nanoTime() + "";
        this.isLongOfMainOffset = true;
    }

    public void parseOffsetStr(String offsetStr) {
        mainOffset = parseOffset(offsetStr, offsetLayers);
    }

    /**
     * 获取offset字符串，通过.把主offset和子offset串接在一起
     *
     * @return
     */
    public String getOffsetStr() {
        String[] offsets = new String[offsetLayers.size() + 1];
        offsets[0] = mainOffset;
        int index = 1;
        for (Long subOffset : offsetLayers) {
            offsets[index] = subOffset + "";
            index++;
        }
        return MapKeyUtil.createKeyBySign(SPLIT_SIGN, offsets);
    }

    /**
     * 增加一个子offset
     *
     * @param index
     */
    public void addLayerOffset(long index) {
        offsetLayers.add(LAYER_OFFST_INIT + index);
    }

    /**
     * 比较当前offset是否大于目标offset
     *
     * @param dstOffset
     * @return
     */
    public boolean greateThan(String dstOffset) {
        return greateThan(getOffsetStr(), dstOffset, isLongOfMainOffset);
    }

    /**
     * 比较两个offset，orioffset是否大于dstoffset
     *
     * @param oriOffset
     * @param dstOffset
     * @param isOffsetIsLong
     * @return
     */
    public static boolean greateThan(String oriOffset, String dstOffset, boolean isOffsetIsLong) {
        if (!isOffsetIsLong) {
            return (oriOffset.compareTo(dstOffset) > 0);
        }
        if (StringUtil.isEmpty(dstOffset)) {
            return true;
        }

        List<Long> dstOffsetLayers = new ArrayList<>();
        long dstMainOffset = Long.parseLong(parseOffset(dstOffset, dstOffsetLayers));

        List<Long> oriOffsetLayers = new ArrayList<>();
        long oriMainOffset = Long.parseLong(parseOffset(oriOffset, oriOffsetLayers));

        return oriMainOffset > dstMainOffset;

    }

    /**
     * 解析offset 字符串
     *
     * @param offset       offse字符串，格式：mainoffset.layyer1offset.layer2offset
     * @param offsetLayers 不同层次的offset
     * @return
     */
    protected static String parseOffset(String offset, List<Long> offsetLayers) {
        if (StringUtil.isEmpty(offset)) {
            return null;
        }
        int index = offset.indexOf(".");
        String mainOffset = null;
        if (index != -1) {
            String[] values = offset.split("\\.");
            mainOffset = (values[0]);
            for (int i = 1; i < values.length; i++) {
                offsetLayers.add(Long.valueOf(values[i]));
            }
        } else {
            mainOffset = (offset);
        }
        return mainOffset;
    }

    public static void main(String[] args) {
        MessageOffset messageOffset = new MessageOffset(12345);
        messageOffset.addLayerOffset(1);
        messageOffset.addLayerOffset(2);
        String offset = (messageOffset.getOffsetStr());
        messageOffset = new MessageOffset(offset, false);
        System.out.println(messageOffset.greateThan("12345.10000001.10000001"));
    }

    public boolean isLongOfMainOffset() {
        return isLongOfMainOffset;
    }

    public List<Long> getOffsetLayers() {
        return offsetLayers;
    }

    public void setOffsetLayers(List<Long> offsetLayers) {
        this.offsetLayers = offsetLayers;
    }

    public void setLongOfMainOffset(boolean longOfMainOffset) {
        isLongOfMainOffset = longOfMainOffset;
    }

    public String getMainOffset() {
        return mainOffset;
    }

    public void setMainOffset(String mainOffset) {
        this.mainOffset = mainOffset;
    }
}
