package com.my.spark.sxt.core;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import java.util.Arrays;
import java.util.List;

public class AccumulatorsValue {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("AccumulatorsValue");
        JavaSparkContext sc = new JavaSparkContext(conf);

        final Accumulator<Integer> sum = sc.accumulator(0, "Our Accumulator");
        List<Integer> list = Arrays.asList(1,2,3,4,5,6);
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        listRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                sum.add(integer);
            }
        });
        System.out.println(sum.value());

        sc.close();
    }

}
