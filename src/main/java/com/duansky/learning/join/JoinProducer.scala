package com.duansky.learning.join

import java.io.PrintWriter
import java.util

/**
  * Created by DuanSky on 2016/6/16.
  */
object JoinProducer {

  val maxKey = Integer.MAX_VALUE / 3 * 2



  def main(args: Array[String]): Unit ={

    //the cross key that both type a and b will contains.
    val crossKeys = getRandomSet(JoinConfig.crossNum)

    //the keys set of type a and b
    val aKeys = fillKeySet(JoinConfig.aNum,crossKeys)
    val bKeys = fillKeySet(JoinConfig.bNum,crossKeys)

    //the key value pairs of type a and b
    val aMap = aKeys.map((_,"a")).toMap
    val bMap = bKeys.map((_,"b")).toMap

//    aMap.map(x => (x._2, x._1))
    //write the results to files
    println("write data into file")
    writeResultToFile(aMap,JoinConfig.aFilePath)
    writeResultToFile(bMap,JoinConfig.bFilePath)

    //send results to Kafka brokers
    val topics = JoinConfig.topics.split(",")
    println("send data to kafka")
    sendResultToKafka(topics(0),aMap)
    sendResultToKafka(topics(1),bMap)

    printf("data produce done. We have produce %d type a and %d type b, %d cross.\n",aKeys.size,bKeys.size,(aKeys&bKeys).size)
  }


  def getRandomSet(number:Int) :Set[Int]={
    var res :Set[Int] = Set()
    do{
      res = res + scala.util.Random.nextInt(maxKey)
    }while(res.size < number)
    res
  }

  def fillKeySet(number:Int,crossKeys:Set[Int]):Set[Int]={
    var res :Set[Int] = crossKeys
    do{
      res = res + scala.util.Random.nextInt(maxKey)
    }while(res.size < number)
    res
  }

  def writeResultToFile(map :Map[Int,String], path:String)={
    val out = new PrintWriter(path)
    map.foreach(out.println)
    out.close()
  }

  def sendResultToKafka(topic:String,map: Map[Int, String]) = {
    // Zookeeper connection properties
    val props = new util.HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, JoinConfig.brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    map.foreach { x =>
      val message = new ProducerRecord[String, String](topic, x._1.toString, x._2)
      println(x)
      producer.send(message)
    }

  }
}
