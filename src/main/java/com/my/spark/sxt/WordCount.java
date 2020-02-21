package com.my.spark.sxt;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("wc");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 情形1：本地文件
//        String path = "E:\\code\\workSpace\\mylearnObject\\git_directory\\Spark\\Spark_Big_Project\\spark-project\\src\\main\\java\\com\\my\\spark\\sxt\\data\\test.txt";
//        JavaRDD<String> text = sc.textFile(path, 3); // 显示指定Partition数量为3（优先级高）
//        JavaRDD<String> text = sc.textFile(path); // 如果没有显示指定Partition数量，则 按照 local[X] 与 2 之间的最小值确定Partition数量。

        // 情形2：Hadoop文件（需再核对验证）
        String path = "hdfs://hadoop104:9000/xxx/xxx.dat";
//        JavaRDD<String> text = sc.textFile(path，x); // 显示指定Partition数量：x必须≥实际Hadoop文件block个数；如果x<实际Hadoop文件block个数，则按实际Hadoop文件block个数指定。local[X]亦如此。
        JavaRDD<String> text = sc.textFile(path); // 如果没有显示指定Partition数量，有多少个block就指定多少个Partition。
        // 如果 实际Hadoop文件block个数为1，且没有显示指定Partition数量，则按照 local[X] 与 2 之间的最小值确定Partition数量。

        JavaRDD<String> words = text.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" "));
            }
        });
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });
        JavaPairRDD<String, Integer> result =  pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        JavaPairRDD<Integer, String> temp =  result.mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple) throws Exception {
                return new Tuple2<Integer, String>(tuple._2, tuple._1);
            }
        });
        JavaPairRDD<String, Integer> sorted =  temp.sortByKey(false).mapToPair(new PairFunction<Tuple2<Integer,String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple) throws Exception {
                return new Tuple2<String, Integer>(tuple._2, tuple._1);
            }
        });
        sorted.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple) throws Exception {
                System.out.println(tuple);
            }
        });

        sc.close();
    }

}
