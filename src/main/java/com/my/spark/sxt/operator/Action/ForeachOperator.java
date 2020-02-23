package com.my.spark.sxt.operator.Action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class ForeachOperator {

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("ForeachOperator");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> names = Arrays.asList("xuruyun1", "xuruyun2", "xuruyun3", "xuruyun4", "xuruyun5",
                "xuruyun7", "xuruyun8", "xuruyun9", "xuruyun10", "xuruyun11", "xuruyun12");
        JavaRDD<String> staffRDD = sc.parallelize(names);

        JavaRDD<String> staffRDD2 = staffRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String str) throws Exception {
                return Arrays.asList(str.split(" "));
            }
        });

        // Action算子：
        /**
         * foreach也是对每个partition中的iterator时行迭代处理，通过用户传入的function（即函数f）对iterator进行内容的处理。
         * 而不同的是，函数f中的参数传入的不再是一个迭代器，而是每次foreach得到RDD的一个kv实例，也就是具体的数据。
         */
        staffRDD2.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        // Action算子：
        /**
         * foreachPartition是对每个partition中的iterator时行迭代的处理.通过用户传入的function（即函数f）对iterator进行内容的处理，
         * 源码中函数f传入的参数是一个迭代器，也就是说在foreachPartiton中函数处理的是分区迭代器，而非具体的数据。
         */
        staffRDD2.foreachPartition(new VoidFunction<Iterator<String>>() {
            @Override
            public void call(Iterator<String> iterator) throws Exception {
                while(iterator.hasNext()){
                    System.out.println(iterator.next());
                }
            }
        });

        sc.close();
    }

}
