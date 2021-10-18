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
package org.apache.rocketmq.streams.filter.optimization.casewhen;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;

//group by dependent varName list
public class GroupByVarCaseWhen {
   private List<CaseWhenElement> allCaseWhenElements=new ArrayList<>();
   protected int index;
   //key: index in allCaseWhenElement value index in AbstractWhenExpression's allCaseWhenElement
   protected Map<Integer,CaseWhenElement> index2CaseWhenElement=new HashMap<>();

   public GroupByVarCaseWhen(int index){
      this.index=index;
   }
   public void registe(CaseWhenElement caseWhenElement){
      this.allCaseWhenElements.add(caseWhenElement);
      this.index2CaseWhenElement.put(this.getAllCaseWhenElements().size()-1, caseWhenElement);
   }

   public Object executeByResult(IMessage message, AbstractContext context,List<Integer> matchIndexs,boolean executeThen,List<CaseWhenElement> caseWhenElements){
      if(matchIndexs==null){
         return null;
      }
      for(Integer index:matchIndexs){
         CaseWhenElement caseWhenElement=allCaseWhenElements.get(index);
         if(caseWhenElements!=null){
            caseWhenElements.add(caseWhenElement);
         }
         if(executeThen){
            caseWhenElement.executeThen(message,context);
         }
      }
      return null;
   }

   public List<Integer> executeCase(IMessage message, AbstractContext context, boolean isExecuteThen,List<CaseWhenElement> caseWhenElements){
      List<Integer> matchIndexs=new ArrayList<>();
      int index=0;
      for(CaseWhenElement caseWhenElement:this.allCaseWhenElements){
         boolean isMatch=false;
         if(isExecuteThen){
            isMatch=caseWhenElement.executeDirectly(message,context);
         }else {
            isMatch=caseWhenElement.executeCase(message,context);
         }
         if(isMatch){
            if(caseWhenElements!=null){
               caseWhenElements.add(this.index2CaseWhenElement.get(index));
            }
            matchIndexs.add(index);
            return matchIndexs;
         }
         index++;
      }
      return matchIndexs;
   }

   public List<Integer> executeDirectly(IMessage message, AbstractContext context){
      return executeCase(message,context,true,null);
   }
   public Set<String> getDependentFields(){
      Set<String> varNames=new HashSet<>();
      for(CaseWhenElement caseWhenElement:this.allCaseWhenElements){
         varNames.addAll(caseWhenElement.getDependentFields());
      }
      return varNames;
   }

   /**
    * can supprot 254 element ,if the size >254, need remove elements
    * @param size
    * @return
    */
   public List<CaseWhenElement> removeUtilSize(int size){
      if(size()<=size){
         return null;
      }
      int currentSize=size();
      List<CaseWhenElement> all=new ArrayList<>();
      all.addAll(this.getAllCaseWhenElements());
      List<CaseWhenElement> removes=new ArrayList<>();
      for(int i=size;i<currentSize;i--){
         removes.add(this.allCaseWhenElements.get(i));
      }
      for(int i=0;i<size;i++){
         all.add(this.allCaseWhenElements.get(i));
      }
      this.allCaseWhenElements=all;
      return removes;
   }

   public int size(){
      return this.allCaseWhenElements.size();
   }

   public List<CaseWhenElement> getAllCaseWhenElements() {
      return allCaseWhenElements;
   }
}
