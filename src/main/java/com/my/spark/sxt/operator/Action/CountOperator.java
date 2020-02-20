package com.my.spark.sxt.operator.Action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class CountOperator {

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("CountOperator");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> names = Arrays.asList("xuruyun1", "xuruyun2", "xuruyun3", "xuruyun4", "xuruyun5",
                "xuruyun7", "xuruyun8", "xuruyun9", "xuruyun10", "xuruyun11", "xuruyun12");
        JavaRDD<String> staffRDD = sc.parallelize(names);

        long count = staffRDD.count(); // Action操作，发生Shuffle
        System.out.println(count);

        sc.close();
    }

}
