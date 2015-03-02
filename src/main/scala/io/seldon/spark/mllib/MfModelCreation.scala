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
import org.json4s.native.JsonMethods._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}
import scala.util.Random._



/**
 * @author firemanphil
 *         Date: 14/10/2014
 *         Time: 15:16
 *
 */
object MfModelCreation {

  object DataSourceMode extends Enumeration {
    def fromString(s: String): DataSourceMode = {
      if(s.startsWith("local://"))
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

  def main(args: Array[String]) {

    
    val conf = new SparkConf()
      .setAppName("SeldonALS")

    println(conf.getAll.deep)
    if (args.size < 10){
      println("Couldn't start job -- wrong number of args.")
      println("Example usage:")
      System.exit(1)
    }

    val sc = new SparkContext(conf)
    val hadoopConf = sc.hadoopConfiguration
    hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    if (args.size > 10)
    {
      hadoopConf.set("fs.s3n.awsAccessKeyId", args(10));
      hadoopConf.set("fs.s3n.awsSecretAccessKey", args(11));
    }
    val client = args(0)
    val date:Int = args(1).toInt
    val daysOfActions = args(2).toInt
    val rank = args(3).toInt
    val lambda = args(4).toDouble
    val alpha = args(5).toDouble
    val iterations = args(6).toInt
    val zkServer = args(7)
    val inputFilesLocation = args(8)
    val inputDataSourceMode = DataSourceMode.fromString(inputFilesLocation)
    if (inputDataSourceMode == NONE) {
      println("input file location must start with local:// or s3n://")
      sys.exit(1)
    }
    val outputFilesLocation = args(9)
    val outputDataSourceMode = DataSourceMode.fromString(outputFilesLocation)
    if (outputDataSourceMode == NONE) {
      println("output file location must start with local:// or s3n://")
      sys.exit(1)
    }

    val curator = new ZkCuratorHandler(zkServer)
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

    val ranks = List(rank)
    val lambdas = List(lambda)
    val numIters = List(iterations)
    val alphas = List(alpha)
    var bestModel: Option[MatrixFactorizationModel] = None
    var bestMap = Double.MinValue
    var bestRank = 0
    var bestLambda = -1.0
    var bestNumIter = -1
    var bestAlpha = 0
    println("Running tests...")
    println("rank,lambda,iters,alpha,score,time")
    for (rank <- ranks; lambda <- lambdas; numIter <- numIters; alpha <- alphas) {
      val timeFirst = System.currentTimeMillis()
      val model: MatrixFactorizationModel = ALS.trainImplicit(ratings, rank, numIter, lambda, alpha)
      outputModelToFile(model, toOutputResource(outputFilesLocation,outputDataSourceMode), outputDataSourceMode, client,date)
      if(curator.getCurator.getZookeeperClient.blockUntilConnectedOrTimedOut()){
        val ensurePath = new EnsurePath("/"+client+"/mf")
        ensurePath.ensure(curator.getCurator.getZookeeperClient)
        curator.getCurator.setData().forPath("/"+client+"/mf",(outputFilesLocation+date).getBytes())
      }
      println(List(rank,lambda,numIter,alpha,0,System.currentTimeMillis() - timeFirst).mkString(","))
      model.userFeatures.unpersist()
      model.productFeatures.unpersist()
      val rdds = sc.getRDDStorageInfo
      if(sc.getPersistentRDDs !=null) {
        for (rdd <- sc.getPersistentRDDs.values) {
          if (rdd.name != null && (rdd.name.startsWith("product") || rdd.name.startsWith("user"))) {
            rdd.unpersist(true);
          }
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
