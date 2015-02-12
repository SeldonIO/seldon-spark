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
import org.joda.time.format.DateTimeFormat
import java.sql.ResultSet
import scala.collection.mutable.ListBuffer
import io.seldon.spark.SparkUtils

case class ActionConfig(
    local : Boolean = false,
    client : String = "",    
    inputPath : String = "",
    outputPath : String = "",
    awsKey : String = "",
    jdbc : String = "",
    tagAttrId : Int = 9,  
    startDay : Int = 0,
    days : Int = 1,
    awsSecret : String = "",
    numUserTagsToKeep : Int = 400,
    numItemTagsToKeep : Int = 400,
    minNumActionsPerUser : Int = 20,
    maxNumActionsPerUser : Int = 60,
    actionNumToStart : Int = 10)
    
    
class CreateActionFeatures(private val sc : SparkContext,config : ActionConfig) {

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
      
      
      (item,(user,date.getMillis()))
      }
    
    rdd
  }
   
   def getItemTagsFromDb(jdbc : String,tagAttrId : Int) = 
  {
    val sql = "select i.item_id,i.client_item_id,unix_timestamp(first_op),tags.value as tags from items i join item_map_varchar tags on (i.item_id=tags.item_id and tags.attr_id="+tagAttrId.toString()+") where i.item_id>? and i.item_id<?"
    val rdd = new org.apache.spark.rdd.JdbcRDD(
    sc,
    () => {
      Class.forName("com.mysql.jdbc.Driver")
      java.sql.DriverManager.getConnection(jdbc)
    },
    sql,
    0, 999999999, 1,
    (row : ResultSet) => (row.getInt("item_id"),row.getString("tags"))
    )
    rdd
  }


    def run()
    {
      val actionsGlob = config.inputPath + "/" + config.client+"/actions/"+SparkUtils.getS3UnixGlob(config.startDay,config.days)+"/*"
      println("loading actions from "+actionsGlob)
      
      val rddActions = parseJsonActions(actionsGlob)
      
      val rddItems = getItemTagsFromDb(config.jdbc, config.tagAttrId)
      
      val rddCombined = rddActions.join(rddItems)
      
      val minNumActions = config.minNumActionsPerUser
      val maxNumActions = config.maxNumActionsPerUser
      val actionNumToStart = config.actionNumToStart
      // create feature for current item and the user history of items viewed
      val rddFeatures = rddCombined.map{ case (item,((user,time),tags)) => (user,(item,time,tags))}.groupByKey().filter(_._2.size > minNumActions).filter(_._2.size < maxNumActions).flatMapValues{v =>
        val buf = new ListBuffer[String]()
        val sorted = v.toArray.sortBy(_._1)
        var userHistory = Set[String]()
        var items = Set[Int]()
        var c = 0
        for ((item,t,tags) <- sorted)
        {
          if (!items.contains(item))
          {
          var line = new StringBuilder()
          if (c > actionNumToStart)
          {
            var tagSet = Set[String]()
            for(tag <- tags.split(","))
            {
              if (!tagSet.contains(tag) && tag.trim().size > 0)
              {
                line ++= " item_tag_"
                line ++= tag.trim().replaceAll(" ", "_")
                tagSet += tag
              }
            }
            for (tag <- userHistory)
            {
              if (!tagSet.contains(tag) && tag.trim().size > 0)
              {
                line ++= " user_tag_"
                line ++= tag.trim()
              }
            }
            buf.append(line.toString().trim())
          }
          for (tag <- tags.split(","))
          {
            if (tag.trim().size > 0)
              userHistory += tag.trim().replaceAll(" ", "_")
          }
          }
          items += item
          c += 1
         } 
         buf 
      }.cache()
      
      // create a set of ids for all features
      //val rddIds = rddFeatures.flatMap(_._2.split(" ")).distinct().zipWithUniqueId();

      val features = rddFeatures.flatMap(_._2.split(" ")).cache()
      
      val topUserfeatures = features.filter(_.startsWith("user_tag_")).map(f => (f,1)).reduceByKey{case (c1,c2) => (c1+c2)}.map{case (f,c) => (c,f)}
        .sortByKey(false).take(config.numUserTagsToKeep)
      
      val topItemfeatures = features.filter(_.startsWith("item_tag_")).map(f => (f,1)).reduceByKey{case (c1,c2) => (c1+c2)}.map{case (f,c) => (c,f)}
        .sortByKey(false).take(config.numItemTagsToKeep)

      
      var id = 1
      var idMap = collection.mutable.Map[String,Int]()
      for((c,f) <- topUserfeatures)
      {
        idMap.put(f, id)
        id += 1
      }
      for((c,f) <- topItemfeatures)
      {
        idMap.put(f, id)
        id += 1
      }

      //save feature id mapping to file
      val rddIds = sc.parallelize(idMap.toSeq,1)
      val idsOutPath = config.outputPath + "/" + config.client + "/feature_mapping/"+config.startDay
      rddIds.saveAsTextFile(idsOutPath)
      
      val broadcastMap = sc.broadcast(idMap)
      val broadcastTopUserFeatures = sc.broadcast(topUserfeatures.map(v => v._2).toSet)
      val broadcastTopItemFeatures = sc.broadcast(topItemfeatures.map(v => v._2).toSet)
      
      // map strings or orderd list of ids using broadcast map
      val rddFeatureIds = rddFeatures.map{v => 
        val tagToId = broadcastMap.value
        val validUserFeatures = broadcastTopUserFeatures.value
        val validItemFeatures = broadcastTopItemFeatures.value
        val features = v._2.split(" ")
        var line = new StringBuilder(v._1.toString()+" ")
        var ids = ListBuffer[Long]()
        var itemFeaturesFound = false
        for (feature <- features)
        {
          if (validUserFeatures.contains(feature) || validItemFeatures.contains(feature))
            ids.append(tagToId(feature))
          if (!itemFeaturesFound && validItemFeatures.contains(feature))
          {
            itemFeaturesFound = true
          }
        }
        if (itemFeaturesFound)
        {
          ids = ids.sorted
          for (id <- ids)
          {
            line ++= " "
            line ++= id.toString()
            line ++= ":1"
          }
          line.toString().trim()
        }
        else
        {
          ""
        }
        }.filter(_.length()>3)
       
      val outPath = config.outputPath + "/" + config.client + "/features/"+config.startDay
      rddFeatureIds.saveAsTextFile(outPath)
    }
    
    
}


object CreateActionFeatures
{
  def main(args: Array[String]) 
  {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val parser = new scopt.OptionParser[ActionConfig]("CreateActionFeatures") {
    head("CreateActionFeatures", "1.x")
    opt[Unit]('l', "local") action { (_, c) => c.copy(local = true) } text("debug mode - use local Master")
    opt[String]('c', "client") required() valueName("<client>") action { (x, c) => c.copy(client = x) } text("client name (will be used as db and folder suffix)")    
    opt[String]('i', "input-path") required() valueName("path url") action { (x, c) => c.copy(inputPath = x) } text("path prefix for input")
    opt[String]('o', "output-path") required() valueName("path url") action { (x, c) => c.copy(outputPath = x) } text("path prefix for output")
    opt[String]('a', "awskey") required() valueName("aws access key") action { (x, c) => c.copy(awsKey = x) } text("aws key")
    opt[String]('s', "awssecret") required() valueName("aws secret") action { (x, c) => c.copy(awsSecret = x) } text("aws secret")
    opt[String]('j', "jdbc") required() valueName("<JDBC URL>") action { (x, c) => c.copy(jdbc = x) } text("jdbc url (to get dimension for all items)")    
    opt[Int]('t', "tagAttrId") required() action { (x, c) =>c.copy(tagAttrId = x) } text("tag attribute id in database")    
    opt[Int]('r', "numdays") required() action { (x, c) =>c.copy(days = x) } text("number of days in past to get actions for")
    opt[Int]("start-day") required() action { (x, c) =>c.copy(startDay = x) } text("start day in unix time")
    opt[Int]("numUserTagsToKeep") required() action { (x, c) =>c.copy(numUserTagsToKeep = x) } text("number of top user tags to keep")
    opt[Int]("numItemTagsToKeep") required() action { (x, c) =>c.copy(numItemTagsToKeep = x) } text("number of top item tags to keep")
    opt[Int]("minNumActionsPerUser") required() action { (x, c) =>c.copy(minNumActionsPerUser = x) } text("min number of actions a user must have")
    opt[Int]("maxNumActionsPerUser") required() action { (x, c) =>c.copy(maxNumActionsPerUser = x) } text("max number of actions a user must have")
    opt[Int]("actionNumToStart") required() action { (x, c) =>c.copy(actionNumToStart = x) } text("wait until this number of actions for a user before creating features")

    }
    
    parser.parse(args, ActionConfig()) map { config =>
    val conf = new SparkConf()
      .setAppName("CreateActionFeatures")
      
    if (config.local)
      conf.setMaster("local")
    .set("spark.executor.memory", "8g")
    
    val sc = new SparkContext(conf)
    try
    {
      sc.hadoopConfiguration.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
      sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", config.awsKey)
      sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", config.awsSecret)

      val cByd = new CreateActionFeatures(sc,config)
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