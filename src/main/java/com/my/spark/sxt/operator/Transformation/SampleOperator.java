package com.my.spark.sxt.operator.Transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import java.util.Arrays;
import java.util.List;

public class SampleOperator {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SampleOperator");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> names = Arrays.asList("xuruyun1", "xuruyun2", "xuruyun3", "xuruyun4", "xuruyun5",
                "xuruyun7", "xuruyun8", "xuruyun9", "xuruyun10", "xuruyun11", "xuruyun12");
        JavaRDD<String> staffRDD = sc.parallelize(names, 3);

        // 第一个参数：false非放回采样，true放回采样；
        staffRDD.sample(false, 0.33, 1L).foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        sc.close();
    }

}
