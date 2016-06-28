package com.duansky.learning.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by DuanSky on 2016/6/16.
  */
object HDFSWordCount {
  def main(args:Array[String]): Unit ={
    val sc = new SparkConf().setMaster("local[2]").setAppName("HDFS Word Count")
    val ssc = new StreamingContext(sc,Seconds(10))

    val path = "E://Programming/Paper/data.txt"
    val lines = ssc.textFileStream(path)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
