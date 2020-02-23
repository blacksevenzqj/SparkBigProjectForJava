package com.my.spark.sxt.streaming.kafka;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

/**
 * 基于Kafka Direct方式的实时wordcount程序
 这种方式有如下优点：
 1、简化并行读取：如果要读取多个partition，不需要创建多个输入DStream然后对它们进行union操作。Spark会创建跟Kafka partition一样多的RDD partition，并且会并行
 从Kafka中读取数据。所以在Kafka partition和RDD partition之间，有一个一对一的映射关系。

 2、高性能：如果要保证零数据丢失，在基于receiver的方式中，需要开启WAL机制。这种方式其实效率低下，因为数据实际上被复制了两份，Kafka自己本身就有高可靠的机制，
 会对数据复制一份，而这里又会复制一份到WAL中。而基于direct的方式，不依赖Receiver，不需要开启WAL机制，只要Kafka中作了数据的复制，那么就可以通过Kafka的副本进行恢复。

 3、一次且仅一次的事务机制：
 基于receiver的方式，是使用Kafka的高阶API来在ZooKeeper中保存消费过的offset的。这是消费Kafka数据的传统方式。这种方式配合着WAL机制可以保证数据
 零丢失的高可靠性，但是却无法保证数据被处理一次且仅一次，可能会处理两次。因为Spark和ZooKeeper之间可能是不同步的。
 基于direct的方式，使用kafka的简单api，Spark Streaming自己就负责追踪消费的offset，并保存在checkpoint中。Spark自己一定是同步的，因此可以保证数据
 是消费一次且仅消费一次。
 */
public class KafkaDirectWordCount {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("KafkaDirectWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // 首先，要创建一份kafka参数map
        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list",
                "192.168.1.135:9092,192.168.1.136:9092,192.168.1.137:9092");

        // 然后，要创建一个set，里面放入，你要读取的topic
        // 这个，就是我们所说的，它自己给你做的很好，可以并行读取多个topic
        Set<String> topics = new HashSet<String>();
        topics.add("WordCount");

        // 创建输入DStream
        JavaPairInputDStream<String, String> lines = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics);

        // 执行wordcount操作
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
                    public Tuple2<String, Integer> call(String word) throws Exception {
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