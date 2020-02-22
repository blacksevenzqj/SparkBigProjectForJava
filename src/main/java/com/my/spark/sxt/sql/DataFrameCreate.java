package com.my.spark.sxt.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;


public class DataFrameCreate {

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("DataFrameCreate");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        String path = "E:\\code\\workSpace\\mylearnObject\\git_directory\\Spark\\Spark_Big_Project\\spark-project\\src\\main\\java\\com\\my\\spark\\sxt\\data\\sql\\student.json";
//        DataFrame df = sqlContext.read().json(path);
        DataFrame df = sqlContext.read().format("json").load(path);
        df.show();
        // 打印元数据
        df.printSchema();
        // 直接查询并计算
        df.select("name").show();
        df.select(df.col("name"), df.col("score").plus(1)).show();
        // 过滤
        df.filter(df.col("score").gt(80)).show();
        // 分组
        df.groupBy("score").count().show();

        String savePath = "E:\\code\\workSpace\\mylearnObject\\git_directory\\Spark\\Spark_Big_Project\\spark-project\\src\\main\\java\\com\\my\\spark\\sxt\\data\\sql\\student.parquet";
//        df.select("name", "score").write().save(savePath);
        df.select("name", "score").write().format("parquet").save(savePath);

        sc.close();
    }

}
