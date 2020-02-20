package com.my.spark.sxt.Transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * reduceByKey = GroupByKey + reduce
 * shuffle 洗牌 = map端 + reduce端
 * spark里面这个reduceByKey在map端自带Combiner（在map端的Partition中就进行局部累加操作，在reduce端就可以减轻运算量）
 * reduceByKey 的效率高于 GroupByKey + 再map做累加
 * reduceByKey 能做sum求和，但不能做average平均（因为在map端的Partition中就进行局部操作）
 */
public class ReduceByKeyOperator {

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("ReduceByKeyOperator"); // Driver节点
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

        staffRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }).foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple) throws Exception {
                System.out.println(tuple._1 + " " + tuple._2());
            }
        });

        sc.close();
    }

}
