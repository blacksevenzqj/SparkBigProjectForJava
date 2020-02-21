package com.my.spark.sxt.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

public class SecondSort {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SecondSort");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String path = "E:\\code\\workSpace\\mylearnObject\\git_directory\\Spark\\Spark_Big_Project\\spark-project\\src\\main\\java\\com\\my\\spark\\sxt\\data\\scores2.txt";
//        JavaRDD<String> text = sc.textFile(path, 3); // 显示指定Partition数量为3（优先级高）
        JavaRDD<String> text = sc.textFile(path); // 如果没有显示指定Partition数量，则 按照 local[X] 与 2 之间的最小值确定Partition数量。

        // SecondSortKey就为了sortByKey算子
        JavaPairRDD<SecondSortKey, String> pairRDD = text.mapToPair(new PairFunction<String, SecondSortKey, String>() {
            @Override
            public Tuple2<SecondSortKey, String> call(String s) throws Exception {
                String[] strs = s.split(" ");
                SecondSortKey sk = new SecondSortKey(Integer.valueOf(strs[0]), Integer.valueOf(strs[2]));
                return new Tuple2<SecondSortKey, String>(sk, s);
            }
        });

        JavaPairRDD<SecondSortKey, String> sortedPair = pairRDD.sortByKey(false);
        JavaRDD<String> results = sortedPair.map(new Function<Tuple2<SecondSortKey, String>, String>() {
            @Override
            public String call(Tuple2<SecondSortKey, String> v1) throws Exception {
                return v1._2;
            }
        });
        results.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

    }

}
