package com.my.spark.sxt.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

public class SaveModeTest {

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SaveModeTest");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        String path = "E:\\code\\workSpace\\mylearnObject\\git_directory\\Spark\\Spark_Big_Project\\spark-project\\src\\main\\java\\com\\my\\spark\\sxt\\data\\sql\\";
        DataFrame df = sqlContext.read().format("json").load(path + "student.json");
        df.printSchema();
        df.show();

        df.save(path + "student.parquet", SaveMode.ErrorIfExists); //存在就报错
//        df.save(path + "student.json", SaveMode.Append);
//        df.save(path + "student.json", SaveMode.Ignore); // 存在就忽略
//        df.save(path + "student.json", SaveMode.Overwrite);

        sc.close();
    }

}
