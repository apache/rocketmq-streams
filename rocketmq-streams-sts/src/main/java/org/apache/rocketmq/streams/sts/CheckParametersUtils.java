package org.apache.rocketmq.streams.sts;


public class CheckParametersUtils<T> {

    public static final <T> T checkIfNullReturnDefault(T t1, T t2) {
        if (t1 == null) {
            return t2;
        } else {
            return t1;
        }
    }

    public static final <T> T checkIfNullThrowException(T t, String ExceptionMsg) {

        if (t == null) {
            throw new NullPointerException("check parameter error : {}. ");
        }
        return t;
    }

    public static final int checkIfNullReturnDefaultInt(Object o, int t) {

        if (o == null) {
            return t;
        }
        return Integer.parseInt(String.valueOf(o));

    }

}
