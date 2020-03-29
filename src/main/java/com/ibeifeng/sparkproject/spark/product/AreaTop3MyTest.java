package com.ibeifeng.sparkproject.spark.product;

import com.alibaba.fastjson.JSONObject;
import com.ibeifeng.sparkproject.constant.Constants;
import com.ibeifeng.sparkproject.dao.ITaskDAO;
import com.ibeifeng.sparkproject.dao.factory.DAOFactory;
import com.ibeifeng.sparkproject.domain.Task;
import com.ibeifeng.sparkproject.util.ParamUtils;
import com.ibeifeng.sparkproject.util.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;

public class AreaTop3MyTest {

    public static void main(String args[]){
        // 创建SparkConf
        SparkConf conf = new SparkConf().setAppName("AreaTop3ProductSpark");
        SparkUtils.setMaster(conf);

        // 构建Spark上下文
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());
//		sqlContext.setConf("spark.sql.shuffle.partitions", "1000");
//		sqlContext.setConf("spark.sql.autoBroadcastJoinThreshold", "20971520");

        // 注册自定义函数
        sqlContext.udf().register("concat_long_string",
                new ConcatLongStringUDF(), DataTypes.StringType);
        sqlContext.udf().register("get_json_object",
                new GetJsonObjectUDF(), DataTypes.StringType);
        sqlContext.udf().register("random_prefix",
                new RandomPrefixUDF(), DataTypes.StringType);
        sqlContext.udf().register("remove_random_prefix",
                new RemoveRandomPrefixUDF(), DataTypes.StringType);
        sqlContext.udf().register("group_concat_distinct",
                new GroupConcatDistinctUDAF());

        // 准备模拟数据
        SparkUtils.mockData(sc, sqlContext);

        // 获取命令行传入的taskid，查询对应的任务参数
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();

        long taskid = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_PRODUCT);
        Task task = taskDAO.findById(taskid);

        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);

        // 查询用户指定日期范围内的点击行为数据（city_id，在哪个城市发生的点击行为）
        // 技术点1：Hive数据源的使用
        JavaPairRDD<Long, Row> cityid2clickActionRDD = AreaTop3FunctionMyTest.getcityid2ClickActionRDDByDate(sqlContext, startDate, endDate);
        System.out.println("cityid2clickActionRDD: " + cityid2clickActionRDD.count());

        // 从MySQL中查询城市信息
        // 技术点2：异构数据源之MySQL的使用
        JavaPairRDD<Long, Row> cityid2cityInfoRDD = AreaTop3FunctionMyTest.getcityid2CityInfoRDD(sqlContext);
        System.out.println("cityid2cityInfoRDD: " + cityid2cityInfoRDD.count());

        // 生成点击商品基础信息临时表
        // 技术点3：将RDD转换为DataFrame，并注册临时表
        AreaTop3FunctionMyTest.generateTempClickProductBasicTable(sqlContext, cityid2clickActionRDD, cityid2cityInfoRDD);


    }


}
