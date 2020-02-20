package com.my.spark.sxt.Action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Arrays;
import java.util.List;

public class CollectOperator {

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
        for(String staffInfo : staffRDD2.collect()){ //将数据收集到Driver节点，慎用！
            System.out.println(staffInfo);
        }

        sc.close();
    }

}
