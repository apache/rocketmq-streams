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
package org.apache.rocketmq.streams.script.function;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.script.ScriptComponent;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.junit.Test;

import java.util.Date;
import java.util.List;

import static junit.framework.TestCase.assertTrue;

public class FunctionTest {
    @Test
    public void testCast() {
        JSONObject message = new JSONObject();
        message.put("name", "Chris");
        message.put("age", 18);
        message.put("date1", "2019-09-04 09:04:04");
        IMessage iMessage = new Message(message);
        FunctionContext context = new FunctionContext(iMessage);
        Object value = ScriptComponent.getInstance().getFunctionService().executeFunction(iMessage, context, "cast", "date1", "'date'");
        assertTrue(Date.class.isInstance(value));
    }

    @Test
    public void testDate() {
        JSONObject message = new JSONObject();
        message.put("name", "Chris");
        message.put("age", 18);
        message.put("from", "2019-07-14 00:00:00");
        message.put("last", "2019-07-14 01:00:00");
        message.put("event_type", "alert");
        String scriptValue = "now=now();nowhh=datefirst(now,'hh');from=dateAdd(last,'hh',-1);";
        List<IMessage> list = ScriptComponent.getInstance().getService().executeScript(message, scriptValue);
        for (int i = 0; i < list.size(); i++) {
            assertTrue(list.get(i).getMessageBody().getString("from").equals("2019-07-14 00:00:00"));
            System.out.println(list.get(i).getMessageBody());
        }

    }

    /**
     * 在导表时常用的语句
     */
    @Test
    public void testExportTabel() {

        JSONObject message = new JSONObject();
        //while result
        message.put("prediction_day", "1970-01-01 09:00:00");
        JSONObject msg = new JSONObject();
        msg.put("name", "chris");
        msg.put("age", 18);
        message.put("msg", msg);

        JSONArray jsonArray = new JSONArray();

        String scriptValue
            = "now=now()  ;  min_day_prediction=dateadd(now,’dd’,-30)  ;  min_day_prediction=datefirst"
            + "(min_day_prediction,’dd’) ;if ( min_day_prediction>=prediction_day )    {   "
            + "prediction_day=min_day_prediction  }  else  {  echo()   }  ;until_day=datefirst(now,’dd’);"
            + "start_day_date=prediction_day;end_day_date=dateadd(start_day_date,’dd’,1); if  (  "
            + "end_day_date>until_day  )   else  {    break=‘false’   }   ;   prediction_day=end_day_date;json_merge"
            + "('msg');rm(msg);";
        List<IMessage> list = ScriptComponent.getInstance().getService().executeScript(message, scriptValue);
        assertTrue(list.size() == 1);
    }

}
