package com.duansky.learning.join

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by DuanSky on 2016/6/17.
  */
object JoinAnalysizer {

  val sparkConf = new SparkConf().setMaster("local[4]").setAppName("JoinAnalysizer")
  val sparkCont = new SparkContext(sparkConf)

  def main(args:Array[String]): Unit ={
    analysisSJandSSJ
  }



  //analysis spark join and spark streaming join
  def analysisSJandSSJ(): Unit ={
    //spark json result
    val sj = sparkCont.textFile(JoinConfig.sparkJoinFilePath + File.separator + "part-*")
    //spark streaming result
    val ssj = sparkCont.textFile(JoinConfig.sparkStreamingJoinFilePath + File.separator + "*" + File.separator + "part-*")
    printf("spark join | spark streaming join | cover : %d | %d | %f\n",sj.count(),ssj.count(),ssj.count()*1.0/sj.count())
  }

//  def combineDuration(path:String) {
//    val rootFile :File = new File(path)
//    rootFile.listFiles.map( subFile => {
//      sparkCont.textFile(subFile.getAbsolutePath + File.separator + "part-00000")
//    })
//  }
}
