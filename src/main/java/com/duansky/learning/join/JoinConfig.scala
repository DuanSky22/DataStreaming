package com.duansky.learning.join

/**
  * Created by DuanSky on 2016/6/17.
  */
object JoinConfig {

  //the number of k-v pairs of type a,b and both.
  val aNum = 1000000; val bNum = 100000; val crossNum = 1000

  //spark streaming config
  val sparkStreamingDuration = 10

  //file path
  val aFilePath = "E://Programming/Paper/data/a.txt"
  val bFilePath = "E://Programming/Paper/data/b.txt"
  val sparkJoinFilePath = "E://Programming/Paper/data/ab"
  val sparkStreamingJoinFilePath = "E://Programming/Paper/data/spark_streaming/"

  //kafka config
  val brokers = "133.133.134.175:9092"
  val zkQuorum = "133.133.134.175:40003"
  val topics = "duansky_a_1,duansky_b_1"
  val group = "duansky22"
  val threadNumber = 1

}
