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
package io.seldon.spark.cluster

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.Rating
import util.Random.nextInt
import java.io.File
import java.sql.{DriverManager,ResultSet}
import collection.mutable.{ HashMap, MultiMap, Set }
import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.joda.time.Period
import io.seldon.spark.SparkUtils


case class DimCount(m : scala.collection.mutable.Map[Int,Int],total : Int)
case class DimPercent(dim : Int,percent : Float)
case class Config(
    local : Boolean = false,
    client : String = "",
    jdbc : String = "",
    inputPath : String = "/seldon-models",
    outputPath : String = "/seldon-models",
    minActionsPerUser : Int = 0,
    delta : Double = 0.1,
    minClusterSize : Int = 200,
    awsKey : String = "",
    awsSecret : String = "",
    startDay : Int = 1,
    days : Int = 1)

class ClusterUsersByDimension(private val sc : SparkContext,config : Config) {
 
  
  /*
   * Get the dimensions from the database
   */
  def getDimsFromDb(jdbc : String) = 
  {
    val rdd = new org.apache.spark.rdd.JdbcRDD(
    sc,
    () => {
      Class.forName("com.mysql.jdbc.Driver")
      java.sql.DriverManager.getConnection(jdbc)
    },
    "select item_id,d.dim_id from item_map_enum i join dimension d on (i.attr_id=d.attr_id and i.value_id=d.value_id) left join cluster_dim_exclude e on (d.dim_id=e.dim_id) left join cluster_attr_exclude e2 on (d.attr_id=e2.attr_id) where e.dim_id is null and e2.attr_id is null and d.dim_id<65535 and item_id > ? AND item_id <= ?",
    0, 999999999, 1,
    (row : ResultSet) => (row.getInt("item_id"),row.getInt("dim_id"))
    )
    rdd
  }
  
   def parseJson(path : String) = {
    
    val rdd = sc.textFile(path).map{line =>
      import org.json4s._
      import org.json4s.native.JsonMethods._
      implicit val formats = DefaultFormats
    
      val json = parse(line)
      val user = (json \ "userid").extract[Int]
      val item = (json \ "itemid").extract[Int]
      (user,item)
      }
    
    rdd
  }
  
  def getFilteredActions(minActions : Int,actions : org.apache.spark.rdd.RDD[(Int,Int)]) = {

    actions.groupBy(_._1).filter(_._2.size >= minActions).flatMap(_._2) // filter users with no enough actions
  }
  
  def getUserDims(actionsFiltered : org.apache.spark.rdd.RDD[(Int,Int)],dimMap : HashMap[Int, Set[Int]] with MultiMap[Int, Int]) = {
    val broadcastDimMap = sc.broadcast(dimMap)
    val userDim = actionsFiltered.flatMap { v => 
      val (user,item) = v
      val dMap = broadcastDimMap.value
      if (dMap.contains(item))
      {
        val buf = new ListBuffer[(Int,Int)]()
        for (d <- dMap(item))
        {
          val contrib = ((user,d))
          //println("Adding "+contrib)
          buf +=  contrib
        }
        buf
      }
      else
        None
    }
    userDim
  }
  
  /**
   * combine user->dim by user to create counts of dimensions for each user
   */
  def getUserDimPercent(userDim : org.apache.spark.rdd.RDD[(Int,Int)],dimPercent : scala.collection.mutable.Map[Int,Float],delta : Double) = {
    val broadcastDimPercent = sc.broadcast(dimPercent)
   
    val userDimPercent = userDim.combineByKey(d => 
      {
        val m = scala.collection.mutable.Map[Int,Int]()
        m.put(d, 1)
        DimCount(m,1)
      },
       (mc : DimCount,d : Int) => 
      {
        val ve = mc.m.getOrElse(d, 0)
        mc.m.put(d, ve + 1)
        DimCount(mc.m,mc.total+1)
      },
      (mc1 : DimCount,mc2 : DimCount) => 
      {
        for((d,c) <- mc1.m)
        {
          val ve = mc2.m.getOrElse(d, 0)
          mc2.m.put(d, ve+c)
        }
        DimCount(mc2.m,mc1.total+mc2.total)
      })
      .flatMap(v =>
        {
          val (user,mc) = v
          val buf = new ListBuffer[(Int,DimPercent)]()
          //println("User "+user+" total:"+mc.total)
          val dimPercent = broadcastDimPercent.value
          for((d,c) <- mc.m)
          {
            val global = dimPercent(d)
            val userPercent = c/mc.total.toFloat
            if (userPercent > (global+delta)) {
            //  println("user:"+user+" dim:"+d+" percent:"+userPercent+" global:"+global)
             buf += ((user,DimPercent(d,userPercent))) 
            }
          
          }
        buf
      })
      
     userDimPercent
  }
  
  def filterUserDimensions(userDimPercent : org.apache.spark.rdd.RDD[(Int,DimPercent)],minClusterSize : Int) = {
    userDimPercent.groupBy(_._2.dim).filter(_._2.size >= minClusterSize).flatMap(_._2)
  }
 
  
  def convertJson(userDimPercent : org.apache.spark.rdd.RDD[(Int,DimPercent)]) = {
    import org.json4s._
    import org.json4s.JsonDSL._
    import org.json4s.jackson.JsonMethods._

    val userJson = userDimPercent.map{v =>
      val (user,dp) = v
      val json = (("user" -> user ) ~
            ("dim" -> dp.dim ) ~
            ("weight" -> dp.percent))
       val jsonText = compact(render(json))    
       jsonText
    }
    userJson
  }
  
  def run()
  {
    //
    // Get map of item->dimension from database
    //
    val dimsRdd = getDimsFromDb(config.jdbc)
    // construct a local  map of item->dimension and store in Broadcast variable    
    val dimMap = new HashMap[Int, Set[Int]] with MultiMap[Int, Int]
    val dims = dimsRdd.collect()
    println("dim array of size "+dims.length)
    for ((i,d) <- dims) dimMap.addBinding(i, d)
    println("Dimension map has size "+dimMap.size) 
    
    
    val glob = config.inputPath + "/" + config.client+"/actions/"+SparkUtils.getS3UnixGlob(config.startDay,config.days)+"/*"
    println("loading from "+glob)

    // parse json from inputs
    val rddJson = parseJson(glob)
    
    // get user->item actions and limit to users with enough actions
    val actionsFiltered = getFilteredActions(config.minActionsPerUser,rddJson)

    // get user->dim from user->item by using map of dimensions for each item
    val userDim = getUserDims(actionsFiltered, dimMap)
    
    val numActions = userDim.count   
    println("number of actions is "+numActions)
    // get the counts for each dimension
    val dimCounts = userDim.map(_._2).map(d => (d,1)).reduceByKey(_ + _).collectAsMap
    // derive the percentage of actions for each dimension
    val dimPercent = scala.collection.mutable.Map[Int,Float]()
    for((d,c) <- dimCounts) dimPercent(d) = c/numActions.toFloat
    println("dimCounts size is "+dimCounts.size)
   //for ((d,p) <- dimPercent)
   //   println(" dim "+d+" percent "+p)

    // get for each user the dimension percentage for dimension larger than the global average by some delta
    val userDimPercent = getUserDimPercent(userDim, dimPercent,config.delta)
    
    println("removing cluster smaller than "+config.minClusterSize)
    val userDimPercentFiltered = filterUserDimensions(userDimPercent, config.minClusterSize).sortByKey()
    //convert to JSON
    val json = convertJson(userDimPercentFiltered)
    
    val outPath = config.outputPath + "/" + config.client + "/cluster/"+config.startDay
    
    json.coalesce(1, false).saveAsTextFile(outPath)
  }
}

object ClusterUsersByDimension
{
  def main(args: Array[String]) 
  {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val parser = new scopt.OptionParser[Config]("ClusterUsersByDimension") {
    head("ClusterUsersByDimension", "1.x")
    opt[Unit]('l', "local") action { (_, c) => c.copy(local = true) } text("local mode - use local Master")
    opt[String]('c', "client") required() valueName("<client>") action { (x, c) => c.copy(client = x) } text("client name (will be used as db and folder suffix)")
    opt[String]('i', "input-path") valueName("path url") action { (x, c) => c.copy(inputPath = x) } text("path prefix for input")
    opt[String]('o', "output-path") valueName("path url") action { (x, c) => c.copy(outputPath = x) } text("path prefix for output")
    opt[String]('j', "jdbc") required() valueName("<JDBC URL>") action { (x, c) => c.copy(jdbc = x) } text("jdbc url (to get dimension for all items)")
    opt[Int]('r', "numdays") action { (x, c) =>c.copy(days = x) } text("number of days in past to get actions for")
    opt[Int]("start-day") action { (x, c) =>c.copy(startDay = x) } text("start day in unix time")
    opt[Int]('m', "minActionsPerUser") action { (x, c) =>c.copy(minActionsPerUser = x) } text("min number of actions per user")
    opt[Int]('z', "minClusterSize") action { (x, c) =>c.copy(minClusterSize = x) } text("min cluster size")
    opt[Double]('d', "delta") action { (x, c) =>c.copy(delta = x) } text("min difference in dim percentage for user to be clustered in dimension")
    opt[String]('a', "awskey") valueName("aws access key") action { (x, c) => c.copy(awsKey = x) } text("aws key")
    opt[String]('s', "awssecret") valueName("aws secret") action { (x, c) => c.copy(awsSecret = x) } text("aws secret")
    }
    
    parser.parse(args, Config()) map { config =>
    val conf = new SparkConf()
      .setAppName("ClusterUsersByDimension")
    
    if (config.local)
      conf.setMaster("local")
      .set("spark.executor.memory", "8g")

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
      val cByd = new ClusterUsersByDimension(sc,config)
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
    
  }
}