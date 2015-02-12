/*
 * Seldon -- open source prediction engine
 * =======================================
 * Copyright 2011-2015 Seldon Technologies Ltd and Rummble Ltd (http://www.seldon.io/)
 *
 **********************************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at       
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ********************************************************************************************** 
*/
package io.seldon.spark.actions;

import io.seldon.spark.actions.ActionData;
import io.seldon.spark.actions.JobUtils;

import java.text.ParseException;

import org.junit.Assert;
import org.junit.Test;

public class JobUtilsTest_ {

    @Test
    public void test_getActionDataFromActionLogLine() {
        String actionLogLine = "2014-09-22T23:21:14Z    actions.live    {\"client\":\"clientname\",\"rectag\":\"default\",\"userid\":\"65799303\",\"itemid\":\"578231\",\"type\":\"1\",\"value\":\"0.0\",\"client_userid\":\"cleintuserid\",\"client_itemid\":\"clientname-4305155\"}";
        ActionData actionData = JobUtils.getActionDataFromActionLogLine(actionLogLine);

        Assert.assertEquals(
                "ActionData[timestamp_utc=2014-09-22T23:21:14Z,client=clientname,client_userid=cleintuserid,userid=65799303,itemid=578231,client_itemid=clientname-4305155,rectag=default,type=1,value=0.0]",
                actionData.toString());
    }

    @Test
    public void test_ActionData_toJson() {
        String actionLogLine = "2014-09-22T21:20:14Z    actions.live    {\"client\":\"clientname\",\"rectag\":\"default\",\"userid\":\"65799303\",\"itemid\":\"578231\",\"type\":\"1\",\"value\":\"0.0\",\"client_userid\":\"cleintuserid\",\"client_itemid\":\"clientname-4305155\"}";
        ActionData actionData = JobUtils.getActionDataFromActionLogLine(actionLogLine);

        // System.out.println(actionData.toJson());
        String json = JobUtils.getJsonFromActionData(actionData);

        Assert.assertEquals(
                "ActionData[timestamp_utc=2014-09-22T21:20:14Z,client=clientname,client_userid=cleintuserid,userid=65799303,itemid=578231,client_itemid=clientname-4305155,rectag=default,type=1,value=0.0]",
                actionData.toString());

        Assert.assertEquals(
                "{\"timestamp_utc\":\"2014-09-22T21:20:14Z\",\"client\":\"clientname\",\"client_userid\":\"cleintuserid\",\"userid\":65799303,\"itemid\":578231,\"client_itemid\":\"clientname-4305155\",\"rectag\":\"default\",\"type\":1,\"value\":0.0}",
                json);

    }

    @Test
    public void test_dateToUnixDays() {

        try {
            long unixDays = JobUtils.dateToUnixDays("20140922");
            Assert.assertEquals(16335, unixDays);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        try {
            long unixDays = JobUtils.dateToUnixDays("20141006");
            Assert.assertEquals(16349, unixDays);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

}
