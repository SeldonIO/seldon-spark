package io.seldon.spark.rdd

import org.apache.spark.rdd.RDD
import java.io.File


 

object FileUtils {

 
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
  
  def outputModelToFile(model: RDD[String],outputFilesLocation:String, outputType:DataSourceMode,filename:String) {
    outputType match {
      case LOCAL => outputModelToLocalFile(model.collect(),outputFilesLocation,filename)
      case S3 => outputModelToS3File(model.collect(), outputFilesLocation, filename)
    }
 }
  
  def outputModelToFile(lines: Array[String],outputFilesLocation:String, outputType:DataSourceMode,filename:String) {
    outputType match {
      case LOCAL => outputModelToLocalFile(lines,outputFilesLocation,filename)
      case S3 => outputModelToS3File(lines, outputFilesLocation, filename)
    }
 }
  
  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }
  
  def outputModelToLocalFile(lines: Array[String], outputFilesLocation: String, filename : String) = {
    new File(outputFilesLocation).mkdirs()
    val userFile = new File(outputFilesLocation+"/"+filename);
    userFile.createNewFile()
    printToFile(userFile){
      p => lines.foreach {
        s => {
          p.println(s)
        }
      }
    }
  }
  
  
   def outputModelToS3File(lines: Array[String], outputFilesLocation: String,  filename : String) = {
     import org.jets3t.service.S3Service
     import org.jets3t.service.impl.rest.httpclient.RestS3Service
     import org.jets3t.service.model.{S3Object, S3Bucket}
     import org.jets3t.service.security.AWSCredentials
     val service: S3Service = new RestS3Service(new AWSCredentials(System.getenv("AWS_ACCESS_KEY_ID"), System.getenv("AWS_SECRET_ACCESS_KEY")))
     val bucketString = outputFilesLocation.split("/")(0)
     val bucket = service.getBucket(bucketString)
     val s3Folder = outputFilesLocation.replace(bucketString+"/","")
     val outBuf = new StringBuffer()
     lines.foreach(u => {
      outBuf.append(u)
      outBuf.append("\n")
    })
     val obj = new S3Object(s3Folder+"/"+filename, outBuf.toString())
  }
  
}