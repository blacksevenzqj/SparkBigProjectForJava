package com.my.spark.sxt.operator.Transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import java.util.*;

public class MapPartitionsWithIndexOperator {

    public static void main(String[] args){
        /**
         * 并行度：
         * 1、资源并行度：即执行线程数，由 setMaster("local[X]") 来设置。
         * 2、RDD数据并行度Partition：
         * 注意：当 没有设置spark.default.parallelism、且没有指定parallelize(names, 8)时，则以 local[X] 设置为准；
         * local[X] 的 X 同时设置 资源并行度 和 RDD数据并行度Partition。
         * 3、RDD数据并行度Partition 最好是 资源并行度 的2到3倍
         */
        // 2、RDD数据并行度Partition设置：
        // 2.3、优先级第三：如果没有设置spark.default.parallelism、且没有指定parallelize(names, 8)，则以local[X]的X设置为准。
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("MapPartitionsWithIndexOperator");

        // 2.2、优先级第二：conf.set("spark.default.parallelism", "X")
        conf.set("spark.default.parallelism", "3");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> names = Arrays.asList("xuruyun", "liangyongqi", "wangfeng", "zhangxueyou", "liudehua");
        // 2.1、优先级第一最高：sc.parallelize(names, 8)
        JavaRDD<String> nameRDD = sc.parallelize(names);

        final Map<String, Integer> scoreMap = new HashMap<String, Integer>();
        scoreMap.put("xuruyun", 150);
        scoreMap.put("liangyongqi", 160);
        scoreMap.put("wangfeng", 170);

        // 有几个Partition就调用几次
        JavaRDD<String> nameWithPartitionsIndex = nameRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<String> iterator) throws Exception {
                List<String> list = new ArrayList<String>();
                while(iterator.hasNext()){
                    String name = iterator.next();
                    String result = index + ":" + name;
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

        sc.close();
    }

}
