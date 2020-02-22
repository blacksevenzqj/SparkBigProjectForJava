package com.my.spark.sxt.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

public class BroadCastValue {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("BroadCastValue");
        JavaSparkContext sc = new JavaSparkContext(conf);

        final int f = 3;
        final Broadcast<Integer> broadcast = sc.broadcast(f);
        List<Integer> list = Arrays.asList(1,2,3,4,5,6);
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        JavaRDD<Integer> results = listRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 * broadcast.getValue();
            }
        });
        results.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });

        sc.close();
    }

}
