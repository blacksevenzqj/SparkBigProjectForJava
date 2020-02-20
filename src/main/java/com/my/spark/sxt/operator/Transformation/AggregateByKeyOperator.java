package com.my.spark.sxt.operator.Transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class AggregateByKeyOperator {

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("AggregateByKeyOperator"); // Driver节点
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String, Integer>> scoreList = Arrays.asList(
                new Tuple2<String, Integer>("xuruyun", 150),
                new Tuple2<String, Integer>("liangjinru", 160),
                new Tuple2<String, Integer>("liudehua", 170),
                new Tuple2<String, Integer>("zhangxueyou", 180),
                new Tuple2<String, Integer>("dakoumaya", 190),
                new Tuple2<String, Integer>("ouyang", 200),
                new Tuple2<String, Integer>("wangfei", 190),
                new Tuple2<String, Integer>("wangfei", 100),
                new Tuple2<String, Integer>("wangfei", 80)
        );

        JavaPairRDD<String, Integer> staffRDD = sc.parallelizePairs(scoreList);

        JavaRDD<String> nameWithPartitionsIndex = staffRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<String, Integer>>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<Tuple2<String, Integer>> iterator) throws Exception {
                List<String> list = new ArrayList<String>();
                while(iterator.hasNext()){
                    Tuple2<String, Integer> tuple = iterator.next();
                    String result = index + ":" + tuple;
                    list.add(result);
                }
                return list.iterator();
            }
        }, true);
        nameWithPartitionsIndex.foreach(new VoidFunction<String>() {
            @Override
            public void call(String str) throws Exception {
                System.out.println(str);
            }
        });


        System.out.println("====================================================");


        // 没有实现 求平均，B没说
        /**
         * 第一个参数：每个Key的初始值
         * 第二个参数Function：在map端逻辑运算
         * 第三个参数Function：在reduce端逻辑运算
         */
        JavaPairRDD<String, Integer> aggregateByKey = staffRDD.aggregateByKey(0, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        List<Tuple2<String, Integer>> list = aggregateByKey.collect();
        for(Tuple2<String, Integer> wc : list){
            System.out.println(wc);
        }

        sc.close();
    }

}
