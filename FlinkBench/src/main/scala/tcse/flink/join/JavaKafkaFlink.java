package tcse.flink.join;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import tcse.flink.join.producer.JoinConfig;
import tcse.flink.join.producer.JoinUtil;

import java.util.Properties;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Created by SkyDream on 2016/6/29.
 */
public class JavaKafkaFlink {

    public static void main(String args[]){
        new JavaKafkaFlink().join();
    }

    Properties prop = new Properties();
    String[] topics = null;
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(JoinConfig.parallel());

    public JavaKafkaFlink(){
        prop.put("bootstrap.servers", JoinConfig.brokers());
        prop.put("group.id", JoinConfig.group());
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDSerializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDSerializer");

        topics = JoinConfig.topics().split(",");
    }

    private DataStream<Tuple2<String,String>> dataStream(String topic){
        return env.addSource(new FlinkKafkaConsumer09<Tuple2<String, String>>(
                topic,
                new KeyValueDeserializationSchema(),
                prop));
    }

    public void join(){

        //delete the old files.
        JoinUtil.deleteDir(JoinConfig.flinkJoinResultFilePath());
        JoinUtil.deleteDir(JoinConfig.flinkJoinTypeAFilePath());
        JoinUtil.deleteDir(JoinConfig.flinkJoinTypeBFilePath());

        //get the stream of kafka.
        DataStream<Tuple2<String,String>> streamA = dataStream(topics[0]);
        DataStream<Tuple2<String,String>> streamB = dataStream(topics[1]);

        //write the data to the files.
        streamA.writeAsText(JoinConfig.flinkJoinTypeAFilePath());
        streamB.writeAsText(JoinConfig.flinkJoinTypeBFilePath());

        streamA.print();
        streamB.print();

        streamA.join(streamB).where(new KeySelector<Tuple2<String,String>, String>() {
            @Override
            public String getKey(Tuple2<String, String> value) throws Exception {
                return value.f0;
            }
        }).equalTo(new KeySelector<Tuple2<String, String>, String>() {
            @Override
            public String getKey(Tuple2<String, String> value) throws Exception {
                return value.f0;
            }
        }).window(TumblingProcessingTimeWindows.of(Time.of(10, SECONDS)))
                .apply(new JoinFunction<Tuple2<String,String>, Tuple2<String,String>, Tuple3<String,String,String>>() {
                    @Override
                    public Tuple3<String,String,String> join(Tuple2<String, String> first, Tuple2<String, String> second) throws Exception {
                        return new Tuple3(first.f0,first.f1,second.f1);
                    }
                }).writeAsText(JoinConfig.flinkJoinResultFilePath());

        try {
            env.execute("Kafka Flink");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
