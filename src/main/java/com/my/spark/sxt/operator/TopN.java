package com.my.spark.sxt.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class TopN {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("TopN");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String path = "E:\\code\\workSpace\\mylearnObject\\git_directory\\Spark\\Spark_Big_Project\\spark-project\\src\\main\\java\\com\\my\\spark\\sxt\\data\\top.txt";
//        JavaRDD<String> text = sc.textFile(path, 3); // 显示指定Partition数量为3（优先级高）
        JavaRDD<String> text = sc.textFile(path); // 如果没有显示指定Partition数量，则 按照 local[X] 与 2 之间的最小值确定Partition数量。

        JavaPairRDD<Integer, String> pairRDD = text.mapToPair(new PairFunction<String, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(String s) throws Exception {
                return new Tuple2<Integer, String>(Integer.valueOf(s), s);
            }
        });

        JavaRDD<String> result = pairRDD.sortByKey(false).map(new Function<Tuple2<Integer,String>, String>() {
            @Override
            public String call(Tuple2<Integer,String> tuple) throws Exception {
                return tuple._2;
            }
        });
        result.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        System.out.println("=====================================");

        JavaRDD<String> result2 = pairRDD.sortByKey(false).flatMap(new FlatMapFunction<Tuple2<Integer, String>, String>() {
            @Override
            public Iterable<String> call(Tuple2<Integer, String> tuple) throws Exception {
                List<String> list = Arrays.asList(tuple._2);
                return list;
            }
        });
        result2.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        System.out.println("=====================================");

        List<String> result3 = pairRDD.sortByKey(false).map(new Function<Tuple2<Integer,String>, String>() {
            @Override
            public String call(Tuple2<Integer,String> tuple) throws Exception {
                return tuple._2;
            }
        }).take(3);
        for(String s : result3){
            System.out.println(s);
        }

    }

}
