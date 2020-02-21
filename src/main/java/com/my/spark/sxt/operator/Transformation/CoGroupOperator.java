package com.my.spark.sxt.operator.Transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

// 注意和 join 之间的区别
public class CoGroupOperator {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("CoGroupOperator");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String, String>> nameList = Arrays.asList(
                new Tuple2<String, String>("1", "xuruyun"),
                new Tuple2<String, String>("2", "liangyongqi"),
                new Tuple2<String, String>("3", "zhangxueyou"),
                new Tuple2<String, String>("4", "wangfei"),
                new Tuple2<String, String>("5", "chenyixun")
        );
        List<Tuple2<String, Integer>> scoreList = Arrays.asList(
                new Tuple2<String, Integer>("1", 150),
                new Tuple2<String, Integer>("1", 160),
                new Tuple2<String, Integer>("2", 160),
                new Tuple2<String, Integer>("3", 170),
                new Tuple2<String, Integer>("4", 180),
                new Tuple2<String, Integer>("5", 190)
        );
        JavaPairRDD<String, String> staffRDD1 = sc.parallelizePairs(nameList);
        JavaPairRDD<String, Integer> staffRDD2 = sc.parallelizePairs(scoreList);

        JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<Integer>>> result = staffRDD1.cogroup(staffRDD2);
        result.foreach(new VoidFunction<Tuple2<String, Tuple2<Iterable<String>, Iterable<Integer>>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<Iterable<String>, Iterable<Integer>>> tuple) throws Exception {
                System.out.println(tuple._1 + " " + tuple._2._1 + ":" + tuple._2._2);
            }
        });

        sc.close();
    }

}
