package tcse.flink.join

import tcse.flink.join.producer.{JavaJoinProducer, JoinConfig, JoinUtil}

/**
  * Created by SkyDream on 2016/7/7.
  */
object TestEngine {

  def main(args:Array[String]): Unit ={
    val COUNT = List(
      "100,100,20","100,100,50",
      "1000,1000,20","1000,1000,200","1000,1000,500",
      "10000,10000,20","10000,10000,200","10000,10000,2000","10000,10000,5000",
      "100000,100000,20","100000,100000,200","100000,100000,2000","100000,100000,20000","100000,100000,50000")

    /*
    TODO
      As we know, when the flink started, we cannot modify the parallel unless
    we stop them and start them again. So the argument below will never be used.
     */
    val PARALLEL = List(
      4
    )

    JoinUtil.deleteDir(JoinConfig.flinkJoinAnalysisFilePath)

    var time = 1000

    COUNT.foreach(count =>{
      PARALLEL.foreach(parallel =>{
        val counts = count.split(",")

        JoinConfig.aNum = counts(0).toInt
        JoinConfig.bNum = counts(1).toInt
        JoinConfig.crossNum = counts(2).toInt

        //start produce data
        new JavaJoinProducer().produce()
        //sleep for a while
        Thread.sleep(time * 5)
        //start analysis result
        FlinkJoinAnalysis.analysis()
      })
      time = time * 3 / 2
    })
  }


}
