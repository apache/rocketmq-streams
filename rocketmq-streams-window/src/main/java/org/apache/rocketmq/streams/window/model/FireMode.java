package org.apache.rocketmq.streams.window.model;

/**
 * the mode for supporting more scene
 *
 * @author arthur
 */

public enum FireMode {

    /**
     * 常规触发（最大延迟时间<=窗口大小）
     */
    NORMAL(0),

    /**
     * 分段触发（最大延迟时间>窗口大小）
     */
    PARTITIONED(1),

    /**
     * 增量触发（最大延迟时间>窗口大小）
     */
    ACCUMULATED(2);

    int value;

    private FireMode(int num) {
        this.value = num;
    }

    public static FireMode valueOf(int theValue) {
        switch (theValue) {
            case 0:
                return NORMAL;
            case 1:
                return PARTITIONED;
            case 2:
                return ACCUMULATED;
            default:
                return null;
        }
    }

}
