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
package io.seldon.spark.features

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd._
import java.io.FileOutputStream
import java.io.ObjectOutputStream
import io.seldon.spark.SparkUtils

case class Config (
    local : Boolean = false,
    client : String = "",
    inputPath : String = "/seldon-models",
    awsKey : String = "",
    awsSecret : String = "",
    outputPath : String = "/seldon-models",
    startDay : Int = 1,
    days : Int = 1,
    minWordCount : Int = 50,
    vectorSize : Int = 30)

class Word2VecJob(private val sc : SparkContext,config : Config) {

  def saveVectors(vectors : org.apache.spark.rdd.RDD[(String,Array[Float])],outpath : String)
  {
    vectors.map{v =>
      val key = v._1.replaceAll(",", "")
      var line = new StringBuilder()
      line ++= key
      for (score <- v._2)
      {
        line ++= ","
        line ++= score.toString()
      }
      line.toString()
    }.saveAsTextFile(outpath)
  }
 
  def run()
  {
    //val modelp = loadModel(config.outputPath)
    val glob = config.inputPath + "/" + config.client+"/sessionitems/"+SparkUtils.getS3UnixGlob(config.startDay,config.days)+"/*"
    println("loading from "+glob)
    
   val input = sc.textFile(glob).map(line => line.split(" ").toSeq)
   val word2vec = new SeldonWord2Vec()
   word2vec.setMinCount(config.minWordCount)
   word2vec.setVectorSize(config.vectorSize)
   val model = word2vec.fit(input)
   val vectors = model.getVectors
   val rdd = sc.parallelize(vectors.toSeq, 200)

   val outPath = config.outputPath + "/" + config.client + "/word2vec/"+config.startDay
   
   saveVectors(rdd,outPath)
   
   //rdd.saveAsObjectFile(config.outputPath)
   
   //sc.objectFile(path, minPartitions)
   
  }
 
}


object Word2VecJob
{
  def main(args: Array[String]) 
  {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val parser = new scopt.OptionParser[Config]("ClusterUsersByDimension") {
    head("CrateVWTopicTraining", "1.x")
    opt[Unit]('l', "local") action { (_, c) => c.copy(local = true) } text("debug mode - use local Master")
    opt[String]('c', "client") required() valueName("<client>") action { (x, c) => c.copy(client = x) } text("client name")
    opt[String]('i', "input-path") valueName("input path") action { (x, c) => c.copy(inputPath = x) } text("input location with sentences")
    opt[String]('o', "output-path") valueName("path url") action { (x, c) => c.copy(outputPath = x) } text("path prefix for output")
    opt[String]('a', "awskey") valueName("aws access key") action { (x, c) => c.copy(awsKey = x) } text("aws key")
    opt[String]('s', "awssecret") valueName("aws secret") action { (x, c) => c.copy(awsSecret = x) } text("aws secret")
    opt[Int]('r', "numdays") action { (x, c) =>c.copy(days = x) } text("number of days in past to get actions for")
    opt[Int]("start-day") action { (x, c) =>c.copy(startDay = x) } text("start day in unix time")
    opt[Int]("min-word-count") action { (x, c) =>c.copy(minWordCount = x) } text("min count for a token to be included")
    opt[Int]("vector-size") action { (x, c) =>c.copy(vectorSize = x) } text("vector size")    
    }
    
    parser.parse(args, Config()) map { config =>
    val conf = new SparkConf()
      .setAppName("FindShortestPath")
      
    if (config.local)
      conf
      .setMaster("local")
      .set("spark.akka.frameSize", "300")
    
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
      val cByd = new Word2VecJob(sc,config)
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