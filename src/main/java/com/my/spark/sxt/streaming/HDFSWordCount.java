package com.my.spark.sxt.streaming;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;


// HDFS同名文件删除再上传，貌似不会再读取
public class HDFSWordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]") // 可以为 local[1]
                .setAppName("WordCount");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(3));

        // 首先，使用JavaStreamingContext的textFileStream()方法，针对HDFS目录创建输入数据流
        // 相对于JavaReceiverInputDStream，textFileStream没有采用Receiver机制，所以不需要单独一条线程接收流数据。
        JavaDStream<String> lines = jssc.textFileStream("hdfs://spark1:9000/wordcount_dir");

        // 执行wordcount操作
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Iterable<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" "));
            }
        });

        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });

        JavaPairDStream<String, Integer> wordcounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        wordcounts.print(); // Action操作

        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}

