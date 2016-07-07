package tcse.flink.join.producer

/**
  * Created by DuanSky on 2016/6/17.
  */
object JoinConfig {

  //the number of k-v pairs of type a,b and both.
  var aNum = 100000; var bNum = 100000; var crossNum = 20000

  var parallel = 4

  //file path
  val rootPath = "E://Programming/Paper/data/" //the root file path of the data streaming.
  val aFilePath = rootPath + "a.txt" // type a
  val bFilePath = rootPath + "b.txt" // type b

  val flinkRootPath = rootPath + "flink/"
  val flinkJoinTypeAFilePath = flinkRootPath + "a/"
  val flinkJoinTypeBFilePath = flinkRootPath + "b/"
  val flinkJoinResultFilePath = flinkRootPath + "ab/"

  val flinkJoinAnalysisFilePath = flinkRootPath + "result.txt"

  //kafka config
  val brokers = "133.133.61.117:9093"
  val zkQuorum = "133.133.61.117:2183"

  val topics = "a,b"
  val group = "duansky-5"
  val threadNumber = 1

}
