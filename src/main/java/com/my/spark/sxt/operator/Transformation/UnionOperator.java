package com.my.spark.sxt.operator.Transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

public class UnionOperator {

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("UnionOperator");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> names1 = Arrays.asList("xuruyun", "liangyongqi", "wangfeng", "zhangxueyou", "liudehua");
        JavaRDD<String> nameRDD1 = sc.parallelize(names1);

        List<String> names2 = Arrays.asList("xuruyun2", "liangyongqi2", "wangfeng2", "zhangxueyou2", "liudehua2");
        JavaRDD<String> nameRDD2 = sc.parallelize(names2);

        nameRDD1.union(nameRDD2).foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        sc.close();
    }

}
