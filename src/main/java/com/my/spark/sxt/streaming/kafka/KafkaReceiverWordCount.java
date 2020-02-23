package com.my.spark.sxt.streaming.kafka;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

/**
 * 基于Kafka receiver方式的实时wordcount程序
 */
public class KafkaReceiverWordCount {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("KafkaWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // 使用KafkaUtils.createStream()方法，创建针对Kafka的输入数据流
        Map<String, Integer> topicThreadMap = new HashMap<String, Integer>();
        // 使用多少个线程去拉取topic的数据
        topicThreadMap.put("WordCount", 1);

        // 这里接收的四个参数；第一个：streamingContext
        // 第二个：ZK quorum；   第三个：consumer group id 可以自己写；
        // 第四个：per-topic number of Kafka partitions to consume
        JavaPairReceiverInputDStream<String, String> lines = KafkaUtils.createStream(
                jssc,
                "192.168.1.135:2181,192.168.1.136:2181,192.168.1.137:2181",
                "DefaultConsumerGroup",
                topicThreadMap);

        // wordcount逻辑
        JavaDStream<String> words = lines.flatMap(
                new FlatMapFunction<Tuple2<String,String>, String>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Iterable<String> call(Tuple2<String, String> tuple)
                            throws Exception {
                        return Arrays.asList(tuple._2.split(" "));
                    }
                });

        JavaPairDStream<String, Integer> pairs = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Tuple2<String, Integer> call(String word)
                            throws Exception {
                        return new Tuple2<String, Integer>(word, 1);
                    }
                });

        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                });

        wordCounts.print();

        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }

}
