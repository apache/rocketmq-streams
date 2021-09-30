//package org.apache.rocketmq.streams.client;
//
//import com.alibaba.fastjson.JSONObject;
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.apache.rocketmq.streams.client.source.DataStreamSource;
//import org.apache.rocketmq.streams.client.transform.DataStream;
//import org.apache.rocketmq.streams.common.channel.source.AbstractSupportOffsetResetSource;
//import org.apache.rocketmq.streams.common.component.ComponentCreator;
//import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;
//import org.apache.rocketmq.streams.common.configure.ConfigureFileKey;
//import org.apache.rocketmq.streams.common.context.Message;
//import org.apache.rocketmq.streams.common.functions.MapFunction;
//import org.apache.rocketmq.streams.common.utils.Base64Utils;
//import org.apache.rocketmq.streams.common.utils.ReflectUtil;
//import org.apache.rocketmq.streams.common.utils.StringUtil;
//import org.apache.rocketmq.streams.db.sink.DBSink;
//import org.junit.Test;
//
//import java.io.Serializable;
//import java.util.*;
//import java.util.concurrent.*;
//import java.util.concurrent.atomic.AtomicInteger;
//
///**
// * @description
// */
//public class BaseLineTest {
//
//    static String url = "jdbc:mysql://sonar.mysql.rdstest.tbsite.net:3306/platform-service?serverTimezone=GMT%2b8";
//    static String userName = "platformservice";
//    static String password = "platformservice";
//    static String tableName = "a_logic_table";
//
//    static{
//        ComponentCreator.getProperties().put(ConfigureFileKey.CHECKPOINT_STORAGE_NAME, "db");
//        ComponentCreator.getProperties().put(ConfigureFileKey.JDBC_URL, url);//数据库连接url
//        ComponentCreator.getProperties().put(ConfigureFileKey.JDBC_USERNAME, userName);//用户名
//        ComponentCreator.getProperties().put(ConfigureFileKey.JDBC_PASSWORD, password);//password
//    }
//
//    @Test
//    public void testBatchInsert() {
//        String endPoint = "http://cn-zhangjiakou-2-share.log.aliyuncs.com";
//        String accessId = "LTAIYISKiOl3jt6p";
//        String accessKey = "TtTheCfEt0at3n5VMPbwVMORHXNYdg";
//        String project = "sas-log-filter-proc";
//        String logStore = "proc_start_relationship";
//        String consumeGroup = "dipperBaseLine";
//        //todo savecheckpoint key看一下, soucename + groupname + topic + shardid + namespcace
//
//
//
//        DataStreamSource dataStreamsource = StreamBuilder.dataStream("test_baseline", "baseline_pipeline");
//
//        SLSSource slsChannel = new SLSSource(endPoint, project, logStore, accessId, accessKey, consumeGroup);
//        DataStream datastream = dataStreamsource.from(slsChannel);
//        DataStream map = datastream.map(new MyMapFunction());
//        map.toMultiDB(url, userName, password, "logic_table", "field_ds").start();
//    }
//
//    @Test
//    public void testBatchReader() {
//
//        ComponentCreator.getProperties().put(ConfigureFileKey.CHECKPOINT_STORAGE_NAME, "db");
//        ComponentCreator.getProperties().put(ConfigureFileKey.JDBC_URL, url);//数据库连接url
//        ComponentCreator.getProperties().put(ConfigureFileKey.JDBC_USERNAME, userName);//用户名
//        ComponentCreator.getProperties().put(ConfigureFileKey.JDBC_PASSWORD, password);//password
//
//        DataStreamSource dataStreamsource = StreamBuilder.dataStream("test_baseline", "baseline_pipeline");
//        DataStream datastream = dataStreamsource.fromMultipleDB(url, userName, password, tableName);
//        datastream.toFile("/Users/cyril/workspace/rocketmq-streams/output_file").start();
//
//    }
//
//    @Test
//    public void testAtomicSink() {
//        ComponentCreator.getProperties().put(ConfigureFileKey.CHECKPOINT_STORAGE_NAME, "db");
//        ComponentCreator.getProperties().put(ConfigureFileKey.JDBC_URL, url);//数据库连接url
//        ComponentCreator.getProperties().put(ConfigureFileKey.JDBC_USERNAME, userName);//用户名
//        ComponentCreator.getProperties().put(ConfigureFileKey.JDBC_PASSWORD, password);//password
//        ComponentCreator.getProperties().put(ConfigureFileKey.IS_ATOMIC_DB_SINK, "true");
//
//        DataStreamSource dataStreamsource = StreamBuilder.dataStream("test_baseline", "baseline_pipeline");
//        DataStream datastream = dataStreamsource.fromMultipleDB(url, userName, password, tableName);
//        DBSink sink = new DBSink();
//        sink.setAtomic(true);
//        sink.setTableName("atomic_table");
//        sink.setUrl(url);
//        sink.setUserName(userName);
//        sink.setPassword(password);
//        sink.init();
//        datastream.to(sink).start();
//
//    }
//
//    @Test
//    public void testFromCycleTable() {
//        ComponentCreator.getProperties().put(ConfigureFileKey.CHECKPOINT_STORAGE_NAME, "db");
//        ComponentCreator.getProperties().put(ConfigureFileKey.JDBC_URL, url);//数据库连接url
//        ComponentCreator.getProperties().put(ConfigureFileKey.JDBC_USERNAME, userName);//用户名
//        ComponentCreator.getProperties().put(ConfigureFileKey.JDBC_PASSWORD, password);//password
//        ComponentCreator.getProperties().put(ConfigureFileKey.IS_ATOMIC_DB_SINK, "true");
//
//        ScheduledStreamBuilder builder = new ScheduledStreamBuilder(1, TimeUnit.MINUTES);
//        ScheduledTask task = new ScheduledTask("20210911193600 - 3m", url, userName, password, tableName, "atomic_table");
//        builder.setTask(task);
//        builder.start();
//    }
//
//    @Test
//    public void testThread(){
//        create();
//        System.out.println("abc");
//    }
//
//    public void create(){
//        for(int i = 0; i < 5; i++){
//            Thread thread = new Thread(new Runnable(){
//                @Override
//                public void run(){
//                    for(int i = 0; i < 10000; i++){
//                        System.out.println(Thread.currentThread() + " - " + i);
//                    }
//                }
//            });
//            thread.start();
//        }
//    }
//
//    public static class MyMapFunction implements MapFunction<JSONObject, JSONObject>, Serializable {
//        private static final long serialVersionUID = 5616474032629491606L;
//        @Override
//        public JSONObject map(JSONObject message) throws Exception {
//            String key = message.getString("host_uuid");
//            String value = message.getString("cmdline");
//            String scanTime = message.getString("scan_time");
//            if (scanTime == null) {
//                scanTime = "2021-08-01 00:00:00";
//            }
//            String ds = scanTime.substring(0, 16).replace(" ", "").replaceAll("-", "").replaceAll(":", "") + "00";
//            JSONObject jsonObject = new JSONObject();
//            jsonObject.put("field_key", key + String.valueOf(Math.random()));
//            jsonObject.put("field_value", value.length() > 64 ? value.substring(0, 64) : value);
//            jsonObject.put("field_ds", ds);
//            return jsonObject;
//        }
//    }
//
//    public static class SLSSource extends AbstractSupportOffsetResetSource {
//
//        private static final long serialVersionUID = 5429201258366881915L;
//
//        private static final Log LOG = LogFactory.getLog(SLSSource.class);
//
//        @ENVDependence
//        private String loghubEndPoint;
//
//        @ENVDependence
//        public String project;
//
//        @ENVDependence
//        public String logStore;
//
//        @ENVDependence
//        private String accessId;
//
//        @ENVDependence
//        private String accessKey;
//
//
//        protected String logTimeFieldName;
//
//        private transient ClientWorker clientWorker;
//
//
//        // private static final LogHubCursorPosition cursorPosition = LogHubCursorPosition.END_CURSOR;
//
//        private transient LogHubConfig.ConsumePosition cursorPosition = LogHubConfig.ConsumePosition.END_CURSOR;
//
//        // worker 向服务端汇报心跳的时间间隔，单位是毫秒，建议取值 10000ms。
//        private long heartBeatIntervalMillis = 10000;
//
//        // 是否按序消费
//        private boolean consumeInOrder = true;
//
//        private transient volatile boolean isFinished = false;                                     // 如果消息被销毁，会通过这个标记停止消息的消费
//
//        //sls对应的project和logstore初始化是否完成标志
//        private volatile transient boolean projectAndLogstoreInit = false;
//
//        //sls数据保存周期，默认7天
//        private int ttl = 7;
//
//        //sls shard个数 默认8
//        private int shardCount = 8;
//
//        // 声明一个容量为10000的缓存队列
//        protected transient BlockingQueue<LogGroupData> queue = new LinkedBlockingQueue<LogGroupData>(800);
//        private transient ExecutorService executorService = null;
//        private static transient LogProducer logProducer;
//
//        @Override
//        protected boolean initConfigurable() {
//            if (StringUtil.isEmpty(loghubEndPoint) || StringUtil.isEmpty(accessId) || StringUtil.isEmpty(accessKey)
//                || StringUtil.isEmpty(logStore)) {
//                return false;
//            }
//            //插入前完成sls project与logstore的初始化
//            // initProjectAndLogstore();
//            return true;
//        }
//
//        public SLSSource() {
//        }
//
//        public SLSSource(String loghubEndPoint, String project, String logStore, String accessId, String accessKey,
//                         String groupName) {
//            this.loghubEndPoint = loghubEndPoint;
//            this.project = project;
//            this.logStore = logStore;
//            this.accessId = accessId;
//            this.accessKey = accessKey;
//            this.groupName = groupName;
//        }
//
//        @Override
//        public boolean startSource() {
//            try {
//                destroyCunsumer();
//                executorService = new ThreadPoolExecutor(getMaxThread(), getMaxThread(),
//                    0L, TimeUnit.MILLISECONDS,
//                    new LinkedBlockingQueue<Runnable>(1000));
//                clientWorker = startWork();
//
//
//                return true;
//            } catch (Exception e) {
//                setInitSuccess(false);
//                throw new RuntimeException("start sls channel error " + loghubEndPoint + ":" + project + ":" + logStore, e);
//            }
//        }
//
//        public static void main(String[] args) {
//            System.out.println(Runtime.getRuntime().availableProcessors());
//        }
//
//        protected ClientWorker startWork() {
//            try {
//                LogHubConfig config =
//                    new LogHubConfig(groupName, UUID.randomUUID().toString(), loghubEndPoint, project,
//                        logStore, accessId, accessKey,
//                        cursorPosition);
//                config.setMaxFetchLogGroupSize(getMaxFetchLogGroupSize());
//                ClientWorker worker = new ClientWorker(new LogHubProcessorFactory(this), config);
//                // Thread运行之后，Client Worker会自动运行，ClientWorker扩展了Runnable接口。
//                executorService.execute(worker);
//                return worker;
//            } catch (Exception e) {
//                setInitSuccess(false);
//                throw new RuntimeException("start sls channel error " + loghubEndPoint + ":" + project + ":" + logStore, e);
//            }
//        }
//
//        @Override
//        public boolean supportNewSplitFind() {
//            return false;
//        }
//
//        @Override
//        public boolean supportRemoveSplitFind() {
//            return true;
//        }
//
//        @Override
//        public boolean supportOffsetRest() {
//            return false;
//        }
//
//        @Override
//        protected boolean isNotDataSplit(String queueId) {
//            return false;
//        }
//
//        protected void destroyCunsumer() {
//            super.destroy();
//            isFinished = true;
//            if (clientWorker != null) {
//                try {
//                    clientWorker.shutdown();
//                    Thread.sleep(30 * 1000);
//
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//
//            }
//        }
//
//
//        public LogHubConfig.ConsumePosition getCursorPosition() {
//            return cursorPosition;
//        }
//
//        public String getLoghubEndPoint() {
//            return loghubEndPoint;
//        }
//
//        public void setLoghubEndPoint(String loghubEndPoint) {
//            this.loghubEndPoint = loghubEndPoint;
//        }
//
//        public String getProject() {
//            return project;
//        }
//
//        public void setProject(String project) {
//            this.project = project;
//        }
//
//        public String getLogStore() {
//            return logStore;
//        }
//
//        public void setLogStore(String logStore) {
//            this.logStore = logStore;
//        }
//
//        public String getAccessId() {
//            return accessId;
//        }
//
//        public void setAccessId(String accessId) {
//            this.accessId = accessId;
//        }
//
//        public String getAccessKey() {
//            return accessKey;
//        }
//
//        public void setAccessKey(String accessKey) {
//            this.accessKey = accessKey;
//        }
//
//        public long getHeartBeatIntervalMillis() {
//            return heartBeatIntervalMillis;
//        }
//
//        public void setHeartBeatIntervalMillis(long heartBeatIntervalMillis) {
//            this.heartBeatIntervalMillis = heartBeatIntervalMillis;
//        }
//
//        public boolean isConsumeInOrder() {
//            return consumeInOrder;
//        }
//
//        public void setConsumeInOrder(boolean consumeInOrder) {
//            this.consumeInOrder = consumeInOrder;
//        }
//
//        public boolean isFinished() {
//            return isFinished;
//        }
//
//        public void setFinished(boolean finished) {
//            isFinished = finished;
//        }
//
//        public String getLogTimeFieldName() {
//            return logTimeFieldName;
//        }
//
//        public void setLogTimeFieldName(String logTimeFieldName) {
//            this.logTimeFieldName = logTimeFieldName;
//        }
//
//        public int getTtl() {
//            return ttl;
//        }
//
//        public void setTtl(int ttl) {
//            this.ttl = ttl;
//        }
//
//        public int getShardCount() {
//            return shardCount;
//        }
//
//        public void setShardCount(int shardCount) {
//            this.shardCount = shardCount;
//        }
//
//        public static LogProducer getLogProducer() {
//            return logProducer;
//        }
//
//        public static void setLogProducer(LogProducer logProducer) {
//            SLSSource.logProducer = logProducer;
//        }
//
//
//    }
//
//    static class LogHubProcessorFactory implements ILogHubProcessorFactory {
//
//        private SLSSource channel;
//
//        public LogHubProcessorFactory(SLSSource channel) {
//            this.channel = channel;
//        }
//
//        @Override
//        public ILogHubProcessor generatorProcessor() {
//            // 生成一个消费实例
//            return new LogHubProcessor(channel);
//        }
//    }
//
//    static class LogHubProcessor implements ILogHubProcessor {
//        private static final Log LOG = LogFactory.getLog(LogHubProcessor.class);
//        /**
//         * 相同批次的计数相同
//         */
//        private static ConcurrentHashMap<String, Integer> batchCount = new ConcurrentHashMap();
//        private int mShardId;
//        // 记录上次持久化 check point 的时间
//        private long mLastCheckTime = 0;
//        private int count = 0;
//
//        @Override
//        public void initialize(int shardId) {
//            mShardId = shardId;
//        }
//
//        private SLSSource channel;
//
//        public LogHubProcessor(SLSSource channel) {
//            this.channel = channel;
//        }
//
//        // 消费数据的主逻辑
//        @Override
//        public String process(List<LogGroupData> logGroups, ILogHubCheckPointTracker checkPointTracker) {
//            Long firstOffset = null;
//            int shardId = ReflectUtil.getDeclaredField(checkPointTracker, "shardID");
//            String checkpoint = ReflectUtil.getDeclaredField(checkPointTracker, "cursor");
//            if (logGroups.size() > 0) {
//
//                this.mShardId = shardId;
//
//
//                firstOffset = createOffset(checkpoint);
//                AtomicInteger atomicInteger = new AtomicInteger(0);
//                for (LogGroupData logGroup : logGroups) {
//
//                    List<JSONObject> msgs = doMessage(logGroup, checkPointTracker);
//                    for (int i = 0; i < msgs.size(); i++) {
//                        JSONObject jsonObject = msgs.get(i);
//                        Message msg = channel.createMessage(jsonObject, shardId + "", firstOffset + "", false);
//                        msg.getHeader().addLayerOffset(atomicInteger.incrementAndGet());
//                        //shardOffset.subOffset.add(msg.getHeader().getOffset());
//                        msg.getHeader().setOffsetIsLong(true);
//                        channel.executeMessage(msg);
//                        count++;
//                    }
//                }
//            }
//
//            long curTime = System.currentTimeMillis();
//            //每200ms调用一次
//            // 每隔 60 秒，写一次 check point 到服务端，如果 60 秒内，worker crash，
//            // 新启动的 worker 会从上一个 checkpoint 其消费数据，有可能有重复数据
//            try {
//                if (curTime - mLastCheckTime > channel.getCheckpointTime()) {
//                    count = 0;
//                    mLastCheckTime = curTime;
//                    channel.sendCheckpoint(shardId + "");
//                    checkPointTracker.saveCheckPoint(true);
//                } else {
//                    checkPointTracker.saveCheckPoint(false);
//                }
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//
//
//            // 返回空表示正常处理数据， 如果需要回滚到上个 check point 的点进行重试的话，可以 return checkPointTracker.getCheckpoint()
//            return null;
//        }
//
//        protected Long createOffset(String checkpoint) {
//            try {
//                byte[] bytes = (Base64Utils.decode(checkpoint));
//                String offsetStr = new String(bytes);
//                Long offset = Long.valueOf(offsetStr);
//                return offset;
//            } catch (Exception e) {
//                throw new RuntimeException(e);
//            }
//        }
//
//
//        // 当 worker 退出的时候，会调用该函数，用户可以在此处做些清理工作。
//        @Override
//        public void shutdown(ILogHubCheckPointTracker checkPointTracker) {
//            //Integer shardId = ReflectUtil.getBeanFieldOrJsonValue(checkPointTracker, "shardID");
//            //channel.sendCheckpoint(shardId+"");
//            Set<String> shards = new HashSet<>();
//            shards.add(mShardId + "");
//            this.channel.removeSplit(shards);
//        }
//
//        public List<JSONObject> doMessage(LogGroupData logGroup, ILogHubCheckPointTracker checkPointTracker) {
//            List<JSONObject> messsages = new ArrayList<>();
//            FastLogGroup flg = logGroup.GetFastLogGroup();
//            for (int lIdx = 0; lIdx < flg.getLogsCount(); lIdx++) {
//                String message = null;
//                FastLog log = flg.getLogs(lIdx);
//                if (!channel.getJsonData()) {
//                    if (log.getContentsCount() == 0) {
//                        continue;
//                    }
//                    FastLogContent content = log.getContents(0);
//                    // System.out.println(checkPointTracker.getCheckPoint()+"."+ReflectUtil.getBeanFieldOrJsonValue(logGroup,"offset"));
//                    message = content.getValue();
//                    JSONObject jsonObject = channel.create(message);
//                    //IReliableQuequ.doReliableQueue(reliableQuequ,checkPointTracker,jsonObject);
//                    messsages.add(jsonObject);
//                } else {
//                    JSONObject jsonObject = new JSONObject();
//                    jsonObject.put("logTime", log.getTime());
//                    for (int cIdx = 0; cIdx < log.getContentsCount(); cIdx++) {
//                        FastLogContent content = log.getContents(cIdx);
//                        jsonObject.put(content.getKey(), content.getValue());
//                    }
//
//                    //为了兼容用户自定义类型，不要删除这句
//                    jsonObject = channel.create(jsonObject.toJSONString());
//                    // System.out.println(checkPointTracker.getCheckPoint()+"."+ReflectUtil.getBeanFieldOrJsonValue(logGroup,"offset"));
//                    //IReliableQuequ.doReliableQueue(reliableQuequ,checkPointTracker,jsonObject);
//                    messsages.add(jsonObject);
//                }
//            }
//            return messsages;
//        }
//
//    }
//}