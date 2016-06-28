package com.duansky.learning.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by DuanSky on 2016/6/15.
  */
object NetWorkWordCount {

  def main(args: Array[String]): Unit ={

    println("hello World")

    //create a context with 1 second batch size
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("NewWorkWordCount")
    val ssc = new StreamingContext(sparkConf,Seconds(1))

    //create a DStream
    val lines = ssc.socketTextStream("localhost",9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word,1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
