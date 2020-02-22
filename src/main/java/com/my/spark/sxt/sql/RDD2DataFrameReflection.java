package com.my.spark.sxt.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class RDD2DataFrameReflection {

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("RDD2DataFrameReflection");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        String path = "E:\\code\\workSpace\\mylearnObject\\git_directory\\Spark\\Spark_Big_Project\\spark-project\\src\\main\\java\\com\\my\\spark\\sxt\\data\\sql\\";
        JavaRDD<String> text = sc.textFile(path + "student.txt");

        JavaRDD<Student> studentJavaRDD = text.map(new Function<String, Student>() {
            @Override
            public Student call(String v1) throws Exception {
                String[] str = v1.split(",");
                return new Student(Integer.valueOf(str[0]),str[1],Integer.valueOf(str[2]));
            }
        });

        DataFrame studentDF = sqlContext.createDataFrame(studentJavaRDD, Student.class);
        studentDF.printSchema();
        studentDF.show();

        studentDF.registerTempTable("student");
        DataFrame tempDF = sqlContext.sql("select * from student where age <= 18");

        JavaRDD<Row> sdRDD = tempDF.toJavaRDD();
        JavaRDD<Student> studentRDD = sdRDD.map(new Function<Row, Student>() {
            @Override
            public Student call(Row row) throws Exception {
                int id = row.getAs("id");
                String name = row.getAs("name");
                int age = row.getAs("age");
                Student st = new Student(id, name, age);
                return st;
            }
        });
        studentRDD.foreach(new VoidFunction<Student>() {
            @Override
            public void call(Student student) throws Exception {
                System.out.println(student);
            }
        });

        sc.close();
    }

}
