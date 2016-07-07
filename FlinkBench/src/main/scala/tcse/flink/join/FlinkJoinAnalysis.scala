package tcse.flink.join

import java.io._

import tcse.flink.join.producer.JoinConfig

/**
  * Created by SkyDream on 2016/7/7.
  */
object FlinkJoinAnalysis {


  def getNum(path: String) :Int={
    new File(path).listFiles().map(part => getPartNum(part.getPath)).reduce(_ + _)
  }

  def getPartNum(path: String) :Int={
    val reader:LineNumberReader = new LineNumberReader(new BufferedReader(new InputStreamReader(new FileInputStream(path))))
    var counter:Int = 0
    while(reader.readLine()!=null) counter=counter+1
    reader.close()
    counter
  }

  def writeResult(texts: String*): Unit = {
    val writer:PrintWriter = new PrintWriter(new FileWriter(JoinConfig.flinkJoinAnalysisFilePath,true))
    texts.foreach(text => writer.print(text + " | "))
    writer.println()
    writer.flush()
    writer.close()
  }

  def analysis(): Unit ={
    val sender :String = "(%d,%d)=>%d".format(JoinConfig.aNum,JoinConfig.bNum,JoinConfig.crossNum)
    val receiver :String = "(%d,%d)=>%d".format(
      getNum(JoinConfig.flinkJoinTypeAFilePath),
      getNum(JoinConfig.flinkJoinTypeBFilePath),
      getNum(JoinConfig.flinkJoinResultFilePath))
    writeResult(sender:String,receiver:String)
  }

  def main(args:Array[String]): Unit ={
    analysis()
  }
}
