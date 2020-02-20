package com.my.spark.sxt.operator.Action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class TakeOperator {

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("TakeOperator"); // Driver节点
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String, Integer>> scoreList = Arrays.asList(
                new Tuple2<String, Integer>("xuruyun", 150),
                new Tuple2<String, Integer>("liangjinru", 160),
                new Tuple2<String, Integer>("liudehua", 170),
                new Tuple2<String, Integer>("zhangxueyou", 180),
                new Tuple2<String, Integer>("dakoumaya", 190),
                new Tuple2<String, Integer>("ouyang", 200),
                new Tuple2<String, Integer>("wangfei", 190),
                new Tuple2<String, Integer>("wangfei", 80)
        );

        JavaPairRDD<String, Integer> staffRDD = sc.parallelizePairs(scoreList);

        List<Tuple2<String, Integer>> takeRDD = staffRDD.take(3);
        for(Tuple2 tuple : takeRDD){
            System.out.println(tuple._1 + " " + tuple._2);
        }

        sc.close();
    }

}
