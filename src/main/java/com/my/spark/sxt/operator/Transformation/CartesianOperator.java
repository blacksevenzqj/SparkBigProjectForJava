package com.my.spark.sxt.operator.Transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

// 笛卡尔积
public class CartesianOperator {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("CartesianOperator");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> names1 = Arrays.asList("xuruyun1", "xuruyun2", "xuruyun3", "xuruyun4", "xuruyun5");
        List<String> names2 = Arrays.asList("zhangxueyou1", "zhangxueyou2", "zhangxueyou3", "zhangxueyou4", "zhangxueyou5");

        JavaRDD<String> staffRDD1 = sc.parallelize(names1, 1);
        JavaRDD<String> staffRDD2 = sc.parallelize(names2, 1);

        JavaPairRDD<String, String> pairRDD = staffRDD1.cartesian(staffRDD2);
        pairRDD.foreach(new VoidFunction<Tuple2<String, String>>() {
            @Override
            public void call(Tuple2<String, String> tuple) throws Exception {
                System.out.println(tuple._1 + " " + tuple._2);
            }
        });

        sc.close();
    }

}
