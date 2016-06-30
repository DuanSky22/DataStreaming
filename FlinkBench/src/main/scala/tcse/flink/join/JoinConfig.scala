package tcse.flink.join

/**
  * Created by DuanSky on 2016/6/17.
  */
object JoinConfig {

  //the number of k-v pairs of type a,b and both.
  var aNum = 10000; var bNum = 10000; var crossNum = 20

  //file path
  val baseFilePath = "E://Programming/Paper/data/flink/"

  val kafkaProduceAFilePath = baseFilePath + "a.txt"
  val kafkaProduceBFilePath = baseFilePath + "b.txt"

  val flinkJoinTypeAFilePath = baseFilePath + "join/a/"
  val flinkJoinTypeBFilePath = baseFilePath + "join/b/"
  val flinkJoinResultFilePath = baseFilePath + "join/ab/"

  val finalJoinResultPath = baseFilePath + "result.txt"

  //kafka config
  //caizheng computer
//  val brokers = "133.133.134.13:9092"
//  val zkQuorum = "133.133.134.13:2181"
  //yingying computer
//  val brokers = "133.133.134.175:9092"
//  val zkQuorum = "133.133.134.175:40003"
  //my own computer
  val brokers = "133.133.61.117:9092"
  val zkQuorum = "133.133.61.117:2182"

  val topics = "a-1,b-1"
  val group = "duansky-5"
  val threadNumber = 1

}
