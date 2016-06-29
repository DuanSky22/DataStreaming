package tcse.flink.test

import java.util.Properties

import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

/**
  * Created by DuanSky on 2016/6/29.
  */
object KafkaFlink {

  val prop:Properties = new Properties()
  prop.setProperty("bootstrap.servers", JoinConfig.brokers)
  prop.setProperty("group.id", JoinConfig.group)

  val topics = JoinConfig.topics.split(",")

  val  env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  def dataStream(topic:String):DataStream[String]={
    env.addSource(new FlinkKafkaConsume09<String>(topic, new SimpleStringSchema, prop))
  }

  def main(args:Array[String]): Unit ={
      join()
  }

  def join(): Unit ={
    val streamA = dataStream(topics(0))
    val streamB = dataStream(topics(1))
    val joinResult = streamA.join(streamB)

    streamA.writeAsText(JoinConfig.flinkJoinTypeAFilePath)
    streamB.writeAsText(JoinConfig.flinkJoinTypeBFilePath)

    streamA.print()
    streamB.print()

    env.execute("Kafka Flink")

  }

}
