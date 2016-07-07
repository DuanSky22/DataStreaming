package tcse.join.test.producer

import org.apache.spark.streaming.Seconds

/**
  * Created by DuanSky on 2016/6/17.
  */
object JoinConfig {

  //the number of k-v pairs of type a,b and both.
  var aNum = 100000; var bNum = 100000; var crossNum = 20000

  //spark streaming config
  var sparkStreamingDuration = 5

  //spark streaming window config
  var aLengthTimes = 5 //for window's length must be a multiple of the slide duration.
  var bLengthTimes = 5

  //file path
  val rootPath = "E://Programming/Paper/data/" //the root file path of the data streaming.
  val aFilePath = rootPath + "a.txt" // type a
  val bFilePath = rootPath + "b.txt" // type b

  val sparkRootPath = rootPath + "spark/"

  val sparkJoinFilePath = sparkRootPath + "ab/"

  val sparkStreamingJoinFilePath = sparkRootPath + "spark_streaming/"
  val sparkStreamingJoinTypeAFilePath = sparkStreamingJoinFilePath + "a/"
  val sparkStreamingJoinTypeBFilePath = sparkStreamingJoinFilePath + "b/"
  val sparkStreamingJoinResultFilePath = sparkStreamingJoinFilePath +"ab/"

  val sparkWindowJoinFilePath = sparkRootPath + "spark_window/"
  val sparkWindowJoinTypeAFilePath = sparkWindowJoinFilePath + "a/"
  val sparkWindowJoinTypeBFilePath = sparkWindowJoinFilePath + "b/"
  val sparkWindowJoinResultFilePath = sparkWindowJoinFilePath + "ab/"

  val sparkFinalJoinResultPath = sparkRootPath + "result.txt"

  //kafka config
  val brokers = "133.133.61.117:9092"
  val zkQuorum = "133.133.61.117:2182"

  val topics = "a,b"
  val group = "duansky-5"
  val threadNumber = 1

}
