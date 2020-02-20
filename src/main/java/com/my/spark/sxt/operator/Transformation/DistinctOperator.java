package com.my.spark.sxt.operator.Transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

// distinct有shuffle操作
public class DistinctOperator {

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("DistinctOperator");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> names1 = Arrays.asList("xuruyun", "liangyongqi", "wangfeng", "zhangxueyou", "liudehua", "xuruyun");
        JavaRDD<String> nameRDD1 = sc.parallelize(names1);

        JavaRDD<String> nameRDD2 = nameRDD1.distinct();
        nameRDD2.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        sc.close();
    }

}
