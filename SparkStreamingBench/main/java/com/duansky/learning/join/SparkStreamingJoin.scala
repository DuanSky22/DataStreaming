package com.duansky.learning.join

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by DuanSky on 2016/6/17.
  */
object SparkStreamingJoin {

  def main(args:Array[String]): Unit ={
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("SparkStreamingJoin")
    val ssc = new StreamingContext(sparkConf,Seconds(JoinConfig.sparkStreamingDuration))

    //get stream of type a and b
    val topics = JoinConfig.topics.split(",")
    val aMap = getStream(topics(0),ssc)
    val bMap = getStream(topics(1),ssc)

    val joinRes = aMap.join(bMap).repartition(1)

    //for every test we will delete the old files.
    JoinUtil.deleteDir(JoinConfig.sparkStreamingJoinFilePath)
    joinRes.print()

    joinRes.saveAsTextFiles(JoinConfig.sparkStreamingJoinFilePath)

    ssc.start()
    ssc.awaitTermination()

  }

  def getStream(topic:String,ssc:StreamingContext) :DStream[(String,String)]={
    val topicMap = Map(topic->JoinConfig.threadNumber)
    KafkaUtils.createStream(ssc, JoinConfig.zkQuorum, JoinConfig.group, topicMap)
  }



}
