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
package io.seldon.spark.mllib

import io.seldon.spark.zookeeper.ZkCuratorHandler
import java.io.File
import java.text.SimpleDateFormat
import org.apache.curator.utils.EnsurePath
import org.apache.spark._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.{Rating, ALS, MatrixFactorizationModel}
import org.apache.spark.rdd._
import org.jets3t.service.S3Service
import org.jets3t.service.impl.rest.httpclient.RestS3Service
import org.jets3t.service.model.{S3Object, S3Bucket}
import org.jets3t.service.security.AWSCredentials
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}
import scala.util.Random._
import java.net.URLClassLoader

case class MfConfig(
    client : String = "",
    inputPath : String = "/seldon-models",
    outputPath : String = "/seldon-models",
    startDay : Int = 1,
    days : Int = 1,    
    awsKey : String = "",
    awsSecret : String = "",
    local : Boolean = false,    
    zkHosts : String = "",
    activate : Boolean = false,
    
    rank : Int = 30,
    lambda : Double = 0.1,
    alpha : Double = 1,
    iterations : Int = 2
 )
 

class MfModelCreation(private val sc : SparkContext,config : MfConfig) {

  object DataSourceMode extends Enumeration {
    def fromString(s: String): DataSourceMode = {
      if(s.startsWith("/"))
        return LOCAL
      if(s.startsWith("s3n://"))
        return S3
      return NONE
    }
    type DataSourceMode = Value
    val S3, LOCAL, NONE = Value
  }
  import DataSourceMode._


  def toSparkResource(location:String, mode:DataSourceMode): String = {
    mode match {
      case LOCAL => return location.replace("local:/","")
      case S3 => return location
    }

  }

  def toOutputResource(location:String, mode: DataSourceMode): String = {
    mode match {
      case LOCAL => return location.replace("local:/","")
      case S3 => return location.replace("s3n://", "")
    }
  }
  
  def run() 
  {
    val client = config.client
    val date:Int = config.startDay
    val daysOfActions = config.days
    val rank = config.rank
    val lambda = config.lambda
    val alpha = config.alpha
    val iterations = config.iterations
    val zkServer = config.zkHosts
    val inputFilesLocation = config.inputPath + "/" + config.client + "/actions/"
    val inputDataSourceMode = DataSourceMode.fromString(inputFilesLocation)
    if (inputDataSourceMode == NONE) {
      println("input file location must start with local:// or s3n://")
      sys.exit(1)
    }
    val outputFilesLocation = config.outputPath + "/" + config.client +"/matrix-factorization/"
    val outputDataSourceMode = DataSourceMode.fromString(outputFilesLocation)
    if (outputDataSourceMode == NONE) {
      println("output file location must start with local:// or s3n://")
      sys.exit(1)
    }


    val startTime = System.currentTimeMillis()
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    // set up environment
    val timeStart = System.currentTimeMillis()
    val glob= toSparkResource(inputFilesLocation, inputDataSourceMode) + ((date - daysOfActions + 1) to date).mkString("{", ",", "}")
    println("Looking at "+glob)
    val actions:RDD[(Int, (Int, Int))] = sc.textFile(glob)
      .map { line =>
        val json = parse(line)
        import org.json4s._
        implicit val formats = DefaultFormats
        val user = (json \ "userid").extract[Int]
        val item = (json \ "itemid").extract[Int]
        (item,(user,1))
      }.repartition(2).cache()
    val itemsByCount: RDD[(Int, Int)] = actions.map(x => (x._1, 1)).reduceByKey(_ + _)
    val itemsCount = itemsByCount.count()
    val topQuarter = (itemsCount * 0.25).toInt
    println("total stories " + itemsByCount.count())
    val topItems = sc.broadcast((itemsByCount.top(topQuarter)(Ordering[Int].on[(Int,Int)](_._2))).map(_._1).toSet)

    val productFilteredActions = actions.filter{
      case (product,(_,_)) => topItems.value.contains(product)
    }
    println("productFilteredActions " + productFilteredActions.count())
    val usersByCount = actions.map(x=>(x._2._1,1)).reduceByKey(_+_)
    val usersCount = usersByCount.count()
    val topQuarterUsers = (usersCount * 0.25).toInt
    val topUsers = sc.broadcast(usersByCount.top(topQuarterUsers)(Ordering[Int].on[(Int,Int)](_._2)).map(_._1).toSet)
    println("total users " + usersCount + ", afterfilter users " + topUsers.value.size)

    val allFilteredActions = productFilteredActions.filter{
      case(_,(user,_)) => topUsers.value.contains(user)
    }

    println("allFilteredActions " + allFilteredActions.count())

    val ratings = allFilteredActions.map{
      case (product, (user, rating)) => Rating(user,product,rating)
    }.repartition(2).cache()

    println("before filtering, actions count " + actions.count() + " after filtering " + ratings.count())

   
    
   
    val timeFirst = System.currentTimeMillis()
    val model: MatrixFactorizationModel = ALS.trainImplicit(ratings, rank, iterations, lambda, alpha)
    outputModelToFile(model, toOutputResource(outputFilesLocation,outputDataSourceMode), outputDataSourceMode, client,date)

    if (config.activate)
    {
      val curator = new ZkCuratorHandler(zkServer)
      if(curator.getCurator.getZookeeperClient.blockUntilConnectedOrTimedOut())
      {
        val zkPath = "/all_clients/"+client+"/mf"
        val ensurePath = new EnsurePath(zkPath)
        ensurePath.ensure(curator.getCurator.getZookeeperClient)
        curator.getCurator.setData().forPath(zkPath,(outputFilesLocation+date).getBytes())
      }
    }
    
    println(List(rank,lambda,iterations,alpha,0,System.currentTimeMillis() - timeFirst).mkString(","))

    model.userFeatures.unpersist()
    model.productFeatures.unpersist()
    val rdds = sc.getRDDStorageInfo
    if(sc.getPersistentRDDs !=null) 
    {
        for (rdd <- sc.getPersistentRDDs.values) {
          if (rdd.name != null && (rdd.name.startsWith("product") || rdd.name.startsWith("user"))) {
            rdd.unpersist(true);
          }
        }
    }

    sc.stop()
    println("Time taken " + (System.currentTimeMillis() - startTime))
  }

  def outputModelToLocalFile(model: MatrixFactorizationModel, outputFilesLocation: String, yesterdayUnix: Long) = {
    new File(outputFilesLocation+yesterdayUnix).mkdirs()
    val userFile = new File(outputFilesLocation+yesterdayUnix+"/userFeatures.txt");
    userFile.createNewFile()
    printToFile(userFile){
      p => model.userFeatures.collect().foreach {
        u => {
          p.println(u._1.toString +"|" +u._2.mkString(","))
        }
      }
    }
    val productFile = new File(outputFilesLocation+yesterdayUnix+"/productFeatures.txt");
    productFile.createNewFile()
    printToFile(productFile){
      p => model.productFeatures.collect().foreach {
        u => {
          p.println(u._1.toString +"|" +u._2.mkString(","))
        }
      }
    }
  }

  def outputModelToS3File(model: MatrixFactorizationModel, outputFilesLocation: String, yesterdayUnix: Long) = {
    val service: S3Service = new RestS3Service(new AWSCredentials(System.getenv("AWS_ACCESS_KEY_ID"), System.getenv("AWS_SECRET_ACCESS_KEY")))
    val bucketString = outputFilesLocation.split("/")(0)
    val bucket = service.getBucket(bucketString)
    val s3Folder = outputFilesLocation.replace(bucketString+"/","")
    val outUser = new StringBuffer()
    model.userFeatures.collect().foreach(u => {
      outUser.append(u._1.toString)
      outUser.append("|")
      outUser.append(u._2.mkString(","))
      outUser.append("\n")
    }
    )
    val obj = new S3Object(s3Folder+yesterdayUnix+"/userFeatures.txt", outUser.toString())
    service.putObject(bucket, obj)
    val outProduct = new StringBuffer()
    model.productFeatures.collect().foreach(u => {
      outProduct.append(u._1.toString)
      outProduct.append("|")
      outProduct.append(u._2.mkString(","))
      outProduct.append("\n")
    }
    )
    val objProd = new S3Object(s3Folder+yesterdayUnix+"/productFeatures.txt", outProduct.toString())
    service.putObject(bucket, objProd)
  }

  def outputModelToFile(model: MatrixFactorizationModel,outputFilesLocation:String, outputType:DataSourceMode, client:String, yesterdayUnix: Long) {
    outputType match {
      case LOCAL => outputModelToLocalFile(model,outputFilesLocation,  yesterdayUnix)
      case S3 => outputModelToS3File(model, outputFilesLocation, yesterdayUnix)
    }


  }

  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }
}

/**
 * @author firemanphil
 *         Date: 14/10/2014
 *         Time: 15:16
 *
 */
object MfModelCreation {

 
  def updateConf(config : MfConfig) =
  {
    var c = config.copy()
    if (config.zkHosts.nonEmpty) 
     {
       val curator = new ZkCuratorHandler(config.zkHosts)
       val path = "/all_clients/"+config.client+"/offline/matrix-factorization"
       if (curator.getCurator.checkExists().forPath(path) != null)
       {
         val bytes = curator.getCurator.getData().forPath(path)
         val j = new String(bytes,"UTF-8")
         println("Configuration from zookeeper -> ",j)
         import org.json4s._
         import org.json4s.jackson.JsonMethods._
         implicit val formats = DefaultFormats
         val json = parse(j)
         import org.json4s.JsonDSL._
         import org.json4s.jackson.Serialization.write
         type DslConversion = MfConfig => JValue
         val existingConf = write(c) // turn existing conf into json
         val existingParsed = parse(existingConf) // parse it back into json4s internal format
         val combined = existingParsed merge json // merge with zookeeper value
         c = combined.extract[MfConfig] // extract case class from merged json
         c
       }
       else 
       {
           println("Warning: using default configuration - path["+path+"] not found!");
           c
       }
     }
     else 
     {
       println("Warning: using default configuration - no zkHost!");
       c
     }
  }
  

  def main(args: Array[String]) 
  {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    
    var c = new MfConfig()
    val parser = new scopt.OptionParser[Unit]("MatrixFactorization") {
    head("ClusterUsersByDimension", "1.x")
        opt[Unit]('l', "local") foreach { x => c = c.copy(local = true) } text("local mode - use local Master")
        opt[String]('c', "client") required() valueName("<client>") foreach { x => c = c.copy(client = x) } text("client name (will be used as db and folder suffix)")
        opt[String]('i', "inputPath") valueName("path url") foreach { x => c = c.copy(inputPath = x) } text("path prefix for input")
        opt[String]('o', "outputPath") valueName("path url") foreach { x => c = c.copy(outputPath = x) } text("path prefix for output")
        opt[Int]('r', "days") foreach { x =>c = c.copy(days = x) } text("number of days in past to get foreachs for")
        opt[Int]("startDay") foreach { x =>c = c.copy(startDay = x) } text("start day in unix time")
        opt[String]('a', "awskey") valueName("aws access key") foreach { x => c = c.copy(awsKey = x) } text("aws key")
        opt[String]('s', "awssecret") valueName("aws secret") foreach { x => c = c.copy(awsSecret = x) } text("aws secret")
        opt[String]('z', "zookeeper") valueName("zookeeper hosts") foreach { x => c = c.copy(zkHosts = x) } text("zookeeper hosts (comma separated)")        
        opt[Unit]("activate") foreach { x => c = c.copy(activate = true) } text("activate the model in the Seldon Server")
        
        opt[Int]('u', "rank") foreach { x =>c = c.copy(rank = x) } text("the number of latent factors in the model")
        opt[Double]('m', "lambda") foreach { x =>c = c.copy(lambda = x) } text("the regularization parameter in ALS to stop over-fitting")
        opt[Double]('m', "alpha") foreach { x =>c = c.copy(alpha = x) } text("governs the baseline confidence in preference observations")        
        opt[Int]('u', "iterations") foreach { x =>c = c.copy(iterations = x) } text("the number of iterations to run the modelling")
    }
    
    
    if (parser.parse(args)) // Parse to check and get zookeeper if there
    {
      c = updateConf(c) // update from zookeeper args
      parser.parse(args) // overrride with args that were on command line
      
      val conf = new SparkConf().setAppName("MatrixFactorization")

      if (c.local)
        conf.setMaster("local")
        .set("spark.executor.memory", "8g")

      val sc = new SparkContext(conf)
      try
      {
        sc.hadoopConfiguration.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
        if (c.awsKey.nonEmpty && c.awsSecret.nonEmpty)
        {
         sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", c.awsKey)
         sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", c.awsSecret)
        }
        println(c)
        val mf = new MfModelCreation(sc,c)
        mf.run()
      }
      finally
      {
        println("Shutting down job")
        sc.stop()
      }
   } 
   else 
   {
      
   }


  }

  
}
