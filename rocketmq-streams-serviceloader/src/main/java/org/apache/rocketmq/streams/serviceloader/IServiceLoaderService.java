package org.apache.rocketmq.streams.serviceloader;

import java.util.List;

public interface IServiceLoaderService<T> {

    /**
     * 获取某个指定名称的服务对象
     *
     * @param name
     * @param
     * @return
     */
    T loadService(String name);

    /**
     * 获取多个实现类
     *
     * @param
     * @return
     */
    List<T> loadService();

    /**
     * 如果forceRefresh＝＝false, refresh多次调用只会执行一次； 如果forceRefresh==true,每次调用都会做一次重新扫描
     *
     * @param forceRefresh
     */
    void refresh(boolean forceRefresh);

}
