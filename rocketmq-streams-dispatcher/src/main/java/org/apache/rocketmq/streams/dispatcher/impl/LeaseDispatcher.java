package org.apache.rocketmq.streams.dispatcher.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.streams.common.threadpool.ScheduleFactory;
import org.apache.rocketmq.streams.common.utils.IdUtil;
import org.apache.rocketmq.streams.dispatcher.ICache;
import org.apache.rocketmq.streams.dispatcher.IDispatcher;
import org.apache.rocketmq.streams.dispatcher.IDispatcherCallback;
import org.apache.rocketmq.streams.dispatcher.IStrategy;
import org.apache.rocketmq.streams.dispatcher.entity.DispatcherMapper;
import org.apache.rocketmq.streams.dispatcher.enums.DispatchMode;
import org.apache.rocketmq.streams.dispatcher.strategy.LeastStrategy;
import org.apache.rocketmq.streams.dispatcher.strategy.StrategyFactory;
import org.apache.rocketmq.streams.lease.model.LeaseInfo;
import org.apache.rocketmq.streams.lease.service.ILeaseStorage;
import org.apache.rocketmq.streams.lease.service.impl.LeaseServiceImpl;
import org.apache.rocketmq.streams.lease.service.storages.DBLeaseStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaseDispatcher<T> implements IDispatcher<T> {

    public static final String MAPPER_KEY = "mapper_key";
    public static final String NAMESPACE_CONFIG_SUFFIX = "_config";
    public static final String NAMESPACE_STATUS_SUFFIX = "_status";
    public static final Map<String, TreeSet<String>> local = Maps.newHashMap();
    private static final Logger LOGGER = LoggerFactory.getLogger(LeaseDispatcher.class);
    private final String instanceName;
    private final String dispatchGroup;
    private final DispatchMode dispatchMode;
    private final IDispatcherCallback<T> dispatcherCallback;
    private final ICache cache;
    private final int scheduleTime;

    private final String type;

    protected transient AtomicBoolean isStart = new AtomicBoolean(false);

    protected transient LeaseServiceImpl leaseService;

    public LeaseDispatcher(String jdbcDriver, String url, String userName, String password, String instanceName, String dispatchGroup, DispatchMode dispatchMode, int scheduleTime, IDispatcherCallback<T> dispatcherCallback, ICache cache) {
        this(jdbcDriver, url, userName, password, instanceName, dispatchGroup, dispatchMode, scheduleTime, dispatcherCallback, cache, "job");
    }

    public LeaseDispatcher(String jdbcDriver, String url, String userName, String password, String instanceName, String dispatchGroup, DispatchMode dispatchMode, int scheduleTime, IDispatcherCallback<T> dispatcherCallback, ICache cache, String type) {
        this.dispatchGroup = dispatchGroup;
        this.instanceName = instanceName;
        this.dispatchMode = dispatchMode;
        this.dispatcherCallback = dispatcherCallback;
        this.cache = cache;
        this.scheduleTime = scheduleTime;

        //构建leaseService
        this.leaseService = new LeaseServiceImpl();
        ILeaseStorage storage = new DBLeaseStorage(jdbcDriver, url, userName, password);
        this.leaseService.setLeaseStorage(storage);
        this.type = type;
    }

    @Override
    public void start() throws Exception {
        try {
            if (this.isStart.compareAndSet(false, true)) {

                //启动Master选举
                this.leaseService.startLeaseTask("Master_" + this.dispatchGroup, scheduleTime, nextLeaseDate -> {
                    LOGGER.info("[{}][{}][{}] Select_Master_Success", this.instanceName, this.dispatchGroup, IdUtil.objectId(this));
                });

                ScheduleFactory.getInstance().execute("lease_dispatcher_" + this.instanceName + "_" + this.dispatchGroup, () -> {
                    try {
                        wakeUp();
                        doRunning();
                        LOGGER.info("[{}][{}][{}] Schedule_Execute_Success", this.instanceName, this.dispatchGroup, IdUtil.objectId(this));
                    } catch (Exception e) {
                        LOGGER.error("[{}][{}][{}] Schedule_Execute_Error", this.instanceName, this.dispatchGroup, IdUtil.objectId(this), e);
                    }
                }, 0, scheduleTime / 2, TimeUnit.SECONDS);
                //启动租约的调度任务上报心跳
                this.leaseService.holdLock(dispatchGroup, instanceName, scheduleTime);
            }
        } catch (Exception e) {
            this.isStart.set(false);
            throw new RuntimeException("Dispatcher_Start_Error", e);
        }
    }

    public synchronized void doDispatch() throws Exception {
        TreeSet<String> tmpConsumerIdList = Sets.newTreeSet();
        List<LeaseInfo> consumerList = this.leaseService.queryLockedInstanceByNamePrefix(this.dispatchGroup, "");
        if (consumerList != null && !consumerList.isEmpty()) {
            for (LeaseInfo leaseInfo : consumerList) {
                LOGGER.info("[{}][{}][{}] Instance_List_[{}]", this.instanceName, this.dispatchGroup, IdUtil.objectId(this), leaseInfo.getLeaseUserIp());
                String workId = leaseInfo.getLeaseUserIp().substring(0, leaseInfo.getLeaseUserIp().indexOf(":")).replaceAll("\\.", "_");
                tmpConsumerIdList.add(workId);
            }
            if (type.equalsIgnoreCase("job")) {
                tmpConsumerIdList.remove(IdUtil.workerId());
            }
        }

        TreeSet<String> tasks = Sets.newTreeSet(this.dispatcherCallback.list());//此逻辑必须放在选主之前，因为所有实例都需要加载任务
        if (!tasks.isEmpty()) {
            for (String task : tasks) {
                LOGGER.info("[{}][{}][{}] Task_List_[{}]", this.instanceName, this.dispatchGroup, IdUtil.objectId(this), task);
            }
        }

        if (!tmpConsumerIdList.isEmpty()) {
            TreeSet<String> consumerIdList = local.getOrDefault(this.dispatchGroup + "_" + this.instanceName + "_consumerList", Sets.newTreeSet());
            boolean isConsumerChange = !consumerIdList.equals(tmpConsumerIdList);
            if (isConsumerChange) {
                LOGGER.info("[{}][{}][{}] Cluster_CurrentInstanceList({})_NewInstanceList({})_IsChanged", this.instanceName, this.dispatchGroup, IdUtil.objectId(this), String.join(",", consumerIdList), String.join(",", tmpConsumerIdList));
            }

            if (!tmpConsumerIdList.isEmpty()) {
                TreeSet<String> taskList = local.getOrDefault(this.dispatchGroup + "_" + this.instanceName + "_taskList", Sets.newTreeSet());
                boolean isTaskChange = !taskList.equals(tasks);
                if (isTaskChange) {
                    LOGGER.info("[{}][{}][{}] Task_CurrentTask({})_NewTask({})_IsChanged", this.instanceName, this.dispatchGroup, IdUtil.objectId(this), String.join(", ", taskList), String.join(",", tasks));
                }
                if (isTaskChange || isConsumerChange) {
                    //计算新的调度公式
                    IStrategy iStrategy = StrategyFactory.getStrategy(this.dispatchMode);
                    if (iStrategy instanceof LeastStrategy) {//如果是Least策略，则需要通过cache获取到当前的调度状态
                        String currentMessage = this.cache.getKeyConfig(this.dispatchGroup + NAMESPACE_CONFIG_SUFFIX, MAPPER_KEY);
                        if (currentMessage != null && !currentMessage.isEmpty()) {
                            DispatcherMapper currentDispatcherMapper = DispatcherMapper.parse(currentMessage);
                            ((LeastStrategy)iStrategy).setCurrentDispatcherMapper(currentDispatcherMapper);
                        }
                    }
                    DispatcherMapper dispatcherMapper = iStrategy.dispatch(Lists.newArrayList(tasks), Lists.newArrayList(tmpConsumerIdList));
                    String currentMessage = this.cache.getKeyConfig(this.dispatchGroup + NAMESPACE_CONFIG_SUFFIX, MAPPER_KEY);
                    DispatcherMapper currentDispatcherMapper = DispatcherMapper.parse(currentMessage);
                    if (currentMessage != null && dispatcherMapper.equals(currentDispatcherMapper)) {
                        LOGGER.info("[{}][{}][{}] Dispatcher_Result_Not_Changed", this.instanceName, this.dispatchGroup, IdUtil.objectId(this));
                    } else {
                        LOGGER.info("[{}][{}][{}] Dispatcher_Result_Changed_CurrentResult({})_NewResult({})", this.dispatchGroup, this.instanceName, IdUtil.objectId(this), currentMessage == null ? "{}" : currentMessage, dispatcherMapper);
                        //确认需要推送新的调度列表
                        this.cache.putKeyConfig(this.dispatchGroup + NAMESPACE_CONFIG_SUFFIX, MAPPER_KEY, dispatcherMapper.toString());
                    }
                    local.put(this.dispatchGroup + "_" + this.instanceName + "_consumerList", tmpConsumerIdList);
                    local.put(this.dispatchGroup + "_" + this.instanceName + "_taskList", tasks);
                }
            }
        } else {
            LOGGER.info("[{}][{}][{}] ConsumerList_Empty", this.instanceName, this.dispatchGroup, IdUtil.objectId(this));
        }
    }

    public void doRunning() {
        try {
            //启动前，重新加载一下内存中的任务实例
            this.dispatcherCallback.list();

            String currentMessage = this.cache.getKeyConfig(this.dispatchGroup + NAMESPACE_CONFIG_SUFFIX, MAPPER_KEY);
            DispatcherMapper currentDispatcherMapper = DispatcherMapper.parse(currentMessage);

            Set<String> taskSet = (currentDispatcherMapper == null || currentDispatcherMapper.getTasks(this.instanceName) == null) ? Sets.newHashSet() : currentDispatcherMapper.getTasks(this.instanceName);
            TreeSet<String> localCache = local.getOrDefault(this.dispatchGroup + "_" + this.instanceName + "_local_cache", Sets.newTreeSet());
            LOGGER.info("[{}][{}][{}] Dispatcher({})_Local({})", this.instanceName, this.dispatchGroup, IdUtil.objectId(this), String.join(",", taskSet), String.join(",", localCache));

            TreeSet<String> tmpCache = Sets.newTreeSet();
            tmpCache.addAll(localCache);

            //执行停止指令
            List<String> needStop = (List<String>)CollectionUtils.subtract(tmpCache, taskSet);
            if (!needStop.isEmpty()) {
                List<String> stopSuccess = this.dispatcherCallback.stop(Lists.newArrayList(needStop));
                if (stopSuccess != null && !stopSuccess.isEmpty()) {
                    stopSuccess.forEach(tmpCache::remove);
                }
                LOGGER.info("[{}][{}][{}] Stop_Tasks_NeedStop({})_Stopped({})", this.instanceName, this.dispatchGroup, IdUtil.objectId(this), String.join(",", needStop), stopSuccess == null ? "" : String.join(",", stopSuccess));
            } else {
                LOGGER.info("[{}][{}][{}] No_NeedStop", this.instanceName, this.dispatchGroup, IdUtil.objectId(this));
            }

            //执行启动
            List<String> needStart = (List<String>)CollectionUtils.subtract(taskSet, tmpCache);
            if (!needStart.isEmpty()) {
                List<String> startSuccess = this.dispatcherCallback.start(Lists.newArrayList(needStart));
                if (startSuccess != null && !startSuccess.isEmpty()) {
                    tmpCache.addAll(startSuccess);
                }
                LOGGER.info("[{}][{}][{}] Start_Tasks_NeedStart({})_Started({})", this.instanceName, this.dispatchGroup, IdUtil.objectId(this), String.join(",", needStart), startSuccess == null ? "" : String.join(",", startSuccess));
            } else {
                LOGGER.info("[{}][{}][{}] No_NeedStart", this.instanceName, this.dispatchGroup, IdUtil.objectId(this));
            }
            if (!localCache.equals(tmpCache)) {
                local.put(this.dispatchGroup + "_" + this.instanceName + "_local_cache", tmpCache);
                this.cache.putKeyConfig(this.dispatchGroup + NAMESPACE_STATUS_SUFFIX, this.instanceName, String.join(",", tmpCache));
                LOGGER.info("[{}][{}][{}] Local_Cache_Changed_From({})_To({})", this.instanceName, this.dispatchGroup, IdUtil.objectId(this), String.join(",", localCache), String.join(",", tmpCache));
            }
        } catch (Exception e) {
            LOGGER.error("[{}][{}] Dispatcher_Error", this.instanceName, this.dispatchGroup, e);
        }
    }

    @Override
    public void wakeUp() throws Exception {
        if (this.leaseService.hasLease("Master_" + this.dispatchGroup)) {
            LOGGER.info("[{}][{}][{}] Master_Running", this.instanceName, this.dispatchGroup, IdUtil.objectId(this));
            doDispatch();
        } else {
            LOGGER.info("[{}][{}][{}] Slave_Running", this.instanceName, this.dispatchGroup, IdUtil.objectId(this));
        }
    }

    @Override
    public void close() {
        try {
            if (this.isStart.compareAndSet(true, false)) {
                if (this.leaseService != null) {
                    this.leaseService.unlock(this.dispatchGroup, this.instanceName);
                    this.leaseService.stopLeaseTask("Master_" + this.dispatchGroup);
                }
                ScheduleFactory.getInstance().cancel("lease_dispatcher_" + this.instanceName + "_" + this.dispatchGroup);
                local.clear();
                LOGGER.info("[{}][{}] Dispatcher_Close_Success", this.instanceName, this.dispatchGroup);
            }
        } catch (Exception e) {
            this.isStart.set(true);
            LOGGER.error("[{}][{}] Dispatcher_Close_Error", this.instanceName, this.dispatchGroup, e);
        }
    }

}
