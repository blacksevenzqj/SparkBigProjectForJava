package com.my.spark.sxt.operator.Transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

public class FlatMapOperator {

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("FlatMapOperator");
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

        JavaRDD<String> staffRDD3 = staffRDD2.map(new Function<String, String>() {
            @Override
            public String call(String v1) throws Exception {
                return "Hello " + v1;
            }
        });

        for(String staffInfo : staffRDD3.collect()){ //将数据收集到Driver节点，慎用！
            System.out.println(staffInfo);
        }
        staffRDD3.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        sc.close();
    }

}
