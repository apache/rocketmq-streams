package org.apache.rocketmq.streams.window.sqlcache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.streams.common.channel.sinkcache.IMessageFlushCallBack;
import org.apache.rocketmq.streams.common.channel.sinkcache.impl.AbstractMutilSplitMessageCache;
import org.apache.rocketmq.streams.db.driver.DriverBuilder;
import org.apache.rocketmq.streams.db.driver.JDBCDriver;
import org.apache.rocketmq.streams.db.driver.orm.ORMUtil;
import org.python.icu.impl.coll.BOCSU;

/**
 * cache sql， async and batch commit
 */
public class SQLCache extends AbstractMutilSplitMessageCache<ISQLElement> {
    protected Boolean isOpenCache=true;//if false，then execute sql when receive sql
    protected Set<String> firedWindowInstances=new HashSet<>();//fired window instance ，if the owned sqls have not commit， can cancel the sqls
    protected Map<String,Integer> windowInstance2Index=new HashMap<>();//set index to ISQLElement group by window instance
    protected boolean isLocalOnly;
    public SQLCache(boolean isLocalOnly ){
        super(null);
        this.isLocalOnly=isLocalOnly;
        this.flushCallBack = new MessageFlushCallBack(new SQLCacheCallback());
        this.setBatchSize(1000);
        this.setAutoFlushTimeGap(30*1000);
        this.setAutoFlushSize(100);
        this.openAutoFlush();
    }

    @Override public int addCache(ISQLElement isqlElement) {
        if(isLocalOnly){
            return 0;
        }
        if(isOpenCache==false){
            DriverBuilder.createDriver().execute(isqlElement.getSQL());
            return 1;
        }
        if(isqlElement.isFireNotify()){
            firedWindowInstances.add(isqlElement.getWindowInstanceId());
        }else if(isqlElement.isWindowInstanceSQL()){
            Integer index=windowInstance2Index.get(isqlElement.getWindowInstanceId());
            if(index==null){
                index=0;
            }
            index++;
            isqlElement.setIndex(index);
            windowInstance2Index.put(isqlElement.getWindowInstanceId(),index);
        }

        return super.addCache(isqlElement);
    }

    @Override protected String createSplitId(ISQLElement msg) {
        return msg.getQueueId();
    }

    protected AtomicInteger executeSQLCount=new AtomicInteger(0);
    protected AtomicInteger cancelQLCount=new AtomicInteger(0);
    protected class SQLCacheCallback implements IMessageFlushCallBack<ISQLElement> {
        Set<String> canCancelWindowIntances=new HashSet<>();
        @Override public boolean flushMessage(List<ISQLElement> messages) {
                    List<String> sqls=new ArrayList<>();

                    for(ISQLElement isqlElement:messages){
                        if(isqlElement.isSplitSQL()){
                            sqls.add(isqlElement.getSQL());
                        }else if(isqlElement.isWindowInstanceSQL()){
//                            if(canCancel(isqlElement)){
//                                cancelQLCount.incrementAndGet();
//                                continue;
//                            }else {
                                sqls.add(isqlElement.getSQL());
//                            }
                        }else if(isqlElement.isFireNotify()){
                            windowInstance2Index.remove(isqlElement.getWindowInstanceId());
                            firedWindowInstances.remove(isqlElement.getWindowInstanceId());

                        }
                    }
                    if(sqls.size()==0){
                        return true;
                    }
                    JDBCDriver dataSource = DriverBuilder.createDriver();
                    try {
                        executeSQLCount.addAndGet(sqls.size());
                        dataSource.executSqls(sqls);
                        System.out.println("execute sql count is "+executeSQLCount.get()+";  cancel sql count is "+cancelQLCount.get());
                    } catch (Exception e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    } finally {
                        if (dataSource != null) {
                            dataSource.destroy();
                        }
                    }
                    return true;
                    }


        protected boolean canCancel(ISQLElement element) {
            String windowInstanceId=element.getWindowInstanceId();
            if(!firedWindowInstances.contains(windowInstanceId)){
                return false;
            }
            if(canCancelWindowIntances.contains(windowInstanceId)){
                return true;
            }
            if(element.getIndex()==1){
                canCancelWindowIntances.add(windowInstanceId);
                return true;
            }
            return false;
        }
    }


}
