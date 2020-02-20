package com.my.spark.sxt.operator.Action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import java.util.Arrays;
import java.util.List;

public class ReduceOperator {

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("ReduceOperator"); // Driver节点
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> names = Arrays.asList("xuruyun1", "xuruyun2", "xuruyun3", "xuruyun4", "xuruyun5",
                "xuruyun7", "xuruyun8", "xuruyun9", "xuruyun10", "xuruyun11", "xuruyun12"); // Driver节点

        // 注意：这里在Driver节点指的是：staffRDD的指针（RDD对象）在Driver节点，数据不一定在Driver节点。
        JavaRDD<String> staffRDD = sc.parallelize(names); // Driver节点

        // reduce是Action操作（shuffle操作），是宽依赖，在Driver节点执行。收集数据sum到Driver节点。
        String sum = staffRDD.reduce(new Function2<String, String, String>() {
            // 匿名内部类 new Function2 中的逻辑是在 Executor 中执行的
            @Override
            public String call(String v1, String v2) throws Exception {
                return v1 + "-" + v2;
            }
        });
        System.out.println(sum); // Driver节点

        sc.close();
    }

}
