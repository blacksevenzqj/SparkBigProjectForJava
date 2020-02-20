package com.my.spark.sxt.Transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.*;


public class MapPartitionsOperator {

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("MapPartitionsOperator");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> names = Arrays.asList("xuruyun", "liangyongqi", "wangfeng");
        JavaRDD<String> nameRDD = sc.parallelize(names);

        final Map<String, Integer> scoreMap = new HashMap<String, Integer>();
        scoreMap.put("xuruyun", 150);
        scoreMap.put("liangyongqi", 160);
        scoreMap.put("wangfeng", 170);

        JavaRDD<Integer> scoreRDD = nameRDD.mapPartitions(new FlatMapFunction<Iterator<String>, Integer>() {
            @Override
            public Iterable<Integer> call(Iterator<String> it) throws Exception {
                List<Integer> list = new ArrayList<Integer>();
                while(it.hasNext()){
                    String name = it.next();
                    Integer score = scoreMap.get(name);
                    list.add(score);
                }
                return list;
            }
        });
        scoreRDD.foreach(new VoidFunction<Integer>() { // foreach是在Executor中执行
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });

        sc.close();
    }

}
