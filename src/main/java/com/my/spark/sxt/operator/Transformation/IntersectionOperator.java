package com.my.spark.sxt.operator.Transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import java.util.Arrays;
import java.util.List;

// 求两个RDD中的交集（有去重效果）
public class IntersectionOperator {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("IntersectionOperator");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> names1 = Arrays.asList("xuruyun1", "xuruyun2", "xuruyun3", "xuruyun4", "xuruyun5");
        List<String> names2 = Arrays.asList("xuruyun1", "xuruyun2", "xuruyun3", "xuruyun6", "xuruyun9");

        JavaRDD<String> staffRDD1 = sc.parallelize(names1, 1);
        JavaRDD<String> staffRDD2 = sc.parallelize(names2, 1);

        staffRDD1.intersection(staffRDD2).foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        sc.close();
    }

}
