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
package io.seldon.spark.topics

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import io.seldon.spark.SparkUtils
import java.sql.ResultSet
import scala.collection.mutable.ListBuffer
import org.joda.time.format.DateTimeFormat
import scala.util.Random

case class SessionItemsConfig(
    local : Boolean = false,
    client : String = "",
    inputPath : String = "/seldon-models",
    outputPath : String = "/seldon-models",
    awsKey : String = "",
    awsSecret : String = "",
    startDay : Int = 1,
    days : Int = 1,
    maxIntraSessionGapSecs : Int =  -1,
    minActionsPerUser : Int = 0,
    maxActionsPerUser : Int = 100000)

class SessionItems(private val sc : SparkContext,config : SessionItemsConfig) {

  def parseJsonActions(path : String) = {
    
    val rdd = sc.textFile(path).map{line =>
      import org.json4s._
      import org.json4s.native.JsonMethods._
      implicit val formats = DefaultFormats
      val formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
      val json = parse(line)
      val user = (json \ "userid").extract[Int]
      val item = (json \ "itemid").extract[Int]
      val dateUtc = (json \ "timestamp_utc").extract[String]
      
      val date = formatter.parseDateTime(dateUtc)
      
      
      (user,(item,date.getMillis()))
      }
    
    rdd
  }
  

  
  def run()
  {
    val actionsGlob = config.inputPath + "/" + config.client+"/actions/"+SparkUtils.getS3UnixGlob(config.startDay,config.days)+"/*"
    println("loading actions from "+actionsGlob)
      
    val rddActions = parseJsonActions(actionsGlob)
    
    val minNumActions = config.minActionsPerUser
    val maxNumActions = config.maxActionsPerUser
    val maxGapMsecs = config.maxIntraSessionGapSecs * 1000
      // create feature for current item and the user history of items viewed
    val rddFeatures = rddActions.groupByKey().filter(_._2.size > minNumActions).filter(_._2.size < maxNumActions).flatMapValues{v =>
        val buf = new ListBuffer[String]()
        var line = new StringBuilder()
        val sorted = v.toArray.sortBy(_._2)
        var lastTime : Long = 0
        var timeSecs : Long = 0
        for ((item,t) <- sorted)
        {
          if (lastTime > 0)
          {
            val gap = (t - lastTime)
            if (maxGapMsecs > -1 && gap > maxGapMsecs)
            {
              val lineStr = line.toString().trim()
              if (lineStr.length() > 0)
                buf.append(line.toString().trim())
              line.clear()
            }
          }
          line ++= item.toString() + " "
          lastTime = t
         } 
         val lineStr = line.toString().trim()
         if (lineStr.length() > 0)
           buf.append(lineStr)
         buf 
      }.values
      val outPath = config.outputPath + "/" + config.client + "/sessionitems/"+config.startDay
      rddFeatures.saveAsTextFile(outPath)
  }
}

 object SessionItems
{
  def main(args: Array[String]) 
  {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val parser = new scopt.OptionParser[SessionItemsConfig]("ClusterUsersByDimension") {
    head("CrateVWTopicTraining", "1.x")
    opt[Unit]('l', "local") action { (_, c) => c.copy(local = true) } text("debug mode - use local Master")
    opt[String]('c', "client") required() valueName("<client>") action { (x, c) => c.copy(client = x) } text("client name (will be used as db and folder suffix)")
    opt[String]('i', "input-path") valueName("path url") action { (x, c) => c.copy(inputPath = x) } text("path prefix for input")
    opt[String]('o', "output-path") valueName("path url") action { (x, c) => c.copy(outputPath = x) } text("path prefix for output")
    opt[Int]('r', "numdays") action { (x, c) =>c.copy(days = x) } text("number of days in past to get actions for")
    opt[Int]("start-day") action { (x, c) =>c.copy(startDay = x) } text("start day in unix time")
    opt[String]('a', "awskey") valueName("aws access key") action { (x, c) => c.copy(awsKey = x) } text("aws key")
    opt[String]('s', "awssecret") valueName("aws secret") action { (x, c) => c.copy(awsSecret = x) } text("aws secret")
    opt[Int]('m', "minActionsPerUser") action { (x, c) =>c.copy(minActionsPerUser = x) } text("min number of actions per user")
    opt[Int]('m', "maxActionsPerUser") action { (x, c) =>c.copy(maxActionsPerUser = x) } text("max number of actions per user")    
    opt[Int]('m', "maxSessionGap") action { (x, c) =>c.copy(maxIntraSessionGapSecs = x) } text("max number of secs before assume session over")        
    }
    
    parser.parse(args, SessionItemsConfig()) map { config =>
    val conf = new SparkConf()
      .setAppName("CreateVWTopicTraining")
      
    if (config.local)
      conf.setMaster("local")
    
    val sc = new SparkContext(conf)
    try
    {
      sc.hadoopConfiguration.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
      if (config.awsKey.nonEmpty && config.awsSecret.nonEmpty)
      {
        sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", config.awsKey)
        sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", config.awsSecret)
      }
      println(config)
      val cByd = new SessionItems(sc,config)
      cByd.run()
    }
    finally
    {
      println("Shutting down job")
      sc.stop()
    }
    } getOrElse 
    {
      
    }

    // set up environment

    
  }
}