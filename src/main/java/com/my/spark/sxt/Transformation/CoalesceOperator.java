package com.my.spark.sxt.Transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.*;

// 用于减少Partition，常用于filter之后。
public class CoalesceOperator {

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("MapPartitionsWithIndexOperator");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> names = Arrays.asList("xuruyun1", "xuruyun2", "xuruyun3", "xuruyun4", "xuruyun5",
                "xuruyun7", "xuruyun8", "xuruyun9", "xuruyun10", "xuruyun11", "xuruyun12");
        JavaRDD<String> staffRDD = sc.parallelize(names, 6);

        JavaRDD<String> staffRDD2 = staffRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<String> iterator) throws Exception {
                List<String> list = new ArrayList<String>();
                while(iterator.hasNext()){
                    String staff = iterator.next();
                    list.add("部门[" + (index + 1) + "]" + staff);
                }
                return list.iterator();
            }
        }, true);
        for(String staffInfo : staffRDD2.collect()){
            System.out.println(staffInfo);
        }

        System.out.println("===========================================================");

        // 压缩Partition：
        // RDD.coalesce(x)：默认shuffle=false时，只有Partition在一台机器上的时候才能生效，如果Partition分布在多台机器上不生效。
        // shuffle=true时，Partition所在多台机器进行shuffle代价大。
        JavaRDD<String> staffRDD3 = staffRDD2.coalesce(3);
        JavaRDD<String> staffRDD4 = staffRDD3.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<String> iterator) throws Exception {
                List<String> list = new ArrayList<String>();
                while(iterator.hasNext()){
                    String staff = iterator.next();
                    list.add("新部门[" + (index + 1) + "] + " + staff);
                }
                return list.iterator();
            }
        }, true);
        // RDD.collect()将所有数据都抽到Driver节点上，慎用！而Rdd.foreach()是在集群中操作。
        for(String staffInfo : staffRDD4.collect()){
            System.out.println(staffInfo);
        }

        sc.close();
    }

}
