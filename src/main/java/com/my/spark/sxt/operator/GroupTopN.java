package com.my.spark.sxt.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.*;

public class GroupTopN {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("TopN");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String path = "E:\\code\\workSpace\\mylearnObject\\git_directory\\Spark\\Spark_Big_Project\\spark-project\\src\\main\\java\\com\\my\\spark\\sxt\\data\\scores.txt";
//        JavaRDD<String> text = sc.textFile(path, 3); // 显示指定Partition数量为3（优先级高）
        JavaRDD<String> text = sc.textFile(path); // 如果没有显示指定Partition数量，则 按照 local[X] 与 2 之间的最小值确定Partition数量。


        JavaPairRDD<String, Integer> pairRDD = text.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] arr = s.split(" ");
                return new Tuple2<String, Integer>(arr[0], Integer.valueOf(arr[1]));
            }
        });

        JavaPairRDD<String, Iterable<Integer>> groupPairs = pairRDD.groupByKey();
        // 算子是在Driver节点进行调用，程序的执行都是在各个worker节点上执行。
        JavaPairRDD<String, Iterable<Integer>> top2Scores = groupPairs.mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, String, Iterable<Integer>>() {
            @Override
            public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> tuple) throws Exception {
                List<Integer> list = new ArrayList<Integer>();
                Iterable<Integer> scores = tuple._2;
                Iterator<Integer> it = scores.iterator();
                while(it.hasNext()){
                    Integer score = it.next();
                    list.add(score);
                }
                Collections.sort(list, new Comparator<Integer>() {
                    @Override
                    public int compare(Integer o1, Integer o2) {
                        return -(o1 - o2);
                    }
                });
                list = list.subList(0, 2);
                return new Tuple2<String, Iterable<Integer>>(tuple._1, list);
            }
        });

        top2Scores.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> tuple) throws Exception {
                System.out.println(tuple._1 + " " + tuple._2);
            }
        });

    }

}
