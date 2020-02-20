package com.my.spark.sxt.operator.Transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


/**
 * 我是看日志：如 Executor task launch worker-4] 即为Task数量，0-4共5个。
 * Partition数量 通过 mapPartitionsWithIndex检测。
 */
public class GroupByKeyOperator {

    public static void main(String[] args){
        // 当 没有设置spark.default.parallelism、且没有指定parallelizePairs(names, 8)时，则以 local[X] 设置为准；
        // local[X] 的 X 同时设置 资源并行度 和 RDD数据并行度Partition：5个Partition，5个Task
        SparkConf conf = new SparkConf().setMaster("local[5]").setAppName("GroupByKeyOperator");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String, Integer>> scoreList = Arrays.asList(
                new Tuple2<String, Integer>("xuruyun", 150),
                new Tuple2<String, Integer>("liangjinru", 160),
                new Tuple2<String, Integer>("liudehua", 170),
                new Tuple2<String, Integer>("zhangxueyou", 180),
                new Tuple2<String, Integer>("dakoumaya", 190),
                new Tuple2<String, Integer>("ouyang", 200),
                new Tuple2<String, Integer>("wangfei", 190),
                new Tuple2<String, Integer>("wangfei", 80)
        );
        // 5个Partition，5个Task
        JavaPairRDD<String, Integer> staffRDD = sc.parallelizePairs(scoreList);

        // groupByKey之后还是5个Partition，5个Task
        JavaPairRDD<String, Iterable<Integer>> nameRDD = staffRDD.groupByKey(); // groupByKey 是 宽依赖，shuffle操作
//        nameRDD.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
//            @Override
//            public void call(Tuple2<String, Iterable<Integer>> tuple) throws Exception {
//                System.out.println(tuple._1 + " " + tuple._2);
//            }
//        });
//        System.out.println("----------------------------------------------------");
        JavaRDD<String> nameWithPartitionsIndex = nameRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<String, Iterable<Integer>>>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<Tuple2<String, Iterable<Integer>>> iterator) throws Exception {
                List<String> list = new ArrayList<String>();
                while(iterator.hasNext()){
                    Tuple2<String, Iterable<Integer>> tuple = iterator.next();
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


        // mapToPair是窄依赖
        JavaPairRDD<String, Integer> mapped = staffRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Integer> tuple) throws Exception {
                return new Tuple2<String, Integer>(tuple._1, tuple._2 + 2);
            }
        });
        // 更新为10个Partition，还是5个Task
        JavaPairRDD<String, Integer> result = mapped.repartition(10); // repartition 是 shuffle操作
        // groupByKey之后还是10个Partition，还是5个Task
        JavaPairRDD<String, Iterable<Integer>> finallResult = result.groupByKey(); // groupByKey 是 宽依赖，shuffle操作
//        System.out.println(finallResult.collect());
//        System.out.println("----------------------------------------------------");
        JavaRDD<String> nameWithPartitionsIndex2 = finallResult.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<String, Iterable<Integer>>>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<Tuple2<String, Iterable<Integer>>> iterator) throws Exception {
                List<String> list = new ArrayList<String>();
                while(iterator.hasNext()){
                    Tuple2<String, Iterable<Integer>> tuple = iterator.next();
                    String result = index + ":" + tuple;
                    list.add(result);
                }
                return list.iterator();
            }
        }, true);
        nameWithPartitionsIndex2.foreach(new VoidFunction<String>() {
            @Override
            public void call(String str) throws Exception {
                System.out.println(str);
            }
        });

        sc.close();
    }

}
