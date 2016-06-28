package com.duansky.learning.join

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by DuanSky on 2016/6/16.
  */
object SparkJoin {

  def main(args:Array[String]): Unit ={
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkJoin")
    val sparkCont = new SparkContext(sparkConf)

    println("get text of type a and b...")
    //get text of type a and b
    val aText = sparkCont.textFile(JoinConfig.aFilePath)
    val bText = sparkCont.textFile(JoinConfig.bFilePath)

    val aMap = aText.map(getTuple)
    val bMap = bText.map(getTuple)

    println("join operation of a and b...")
    val joinRes = aMap.join(bMap).repartition(1)
    joinRes.foreach(println)

    //delete the old files
    JoinUtil.deleteDir(JoinConfig.sparkJoinFilePath)
    println("write the result...")
    joinRes.saveAsTextFile(JoinConfig.sparkJoinFilePath)
    printf("spark join done. We have read %d type a and %d type b, %d cross.\n",aMap.count(),bMap.count(),joinRes.count())

  }

  def getTuple(pairs:String) :(String,String) = {
    val tuple = pairs.substring(1,pairs.length-1).split(",")
    (tuple(0),tuple(1))
  }

}