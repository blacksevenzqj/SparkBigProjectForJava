package com.ibeifeng.sparkproject.spark.page;

import com.alibaba.fastjson.JSONObject;
import com.ibeifeng.sparkproject.constant.Constants;
import com.ibeifeng.sparkproject.dao.ITaskDAO;
import com.ibeifeng.sparkproject.dao.factory.DAOFactory;
import com.ibeifeng.sparkproject.domain.Task;
import com.ibeifeng.sparkproject.util.ParamUtils;
import com.ibeifeng.sparkproject.util.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.Date;
import java.util.Map;

public class PageMyTest {

    public static void main(String args[]){
        // 1、构造Spark上下文
        SparkConf conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_PAGE);
        SparkUtils.setMaster(conf);

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());

        // 2、生成模拟数据
        SparkUtils.mockData(sc, sqlContext);

        // 3、查询任务，获取任务的参数
        long taskid = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_PAGE);

        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        Task task = taskDAO.findById(taskid);
        if(task.getTaskid() == 0) {
            System.out.println(new Date() + ": cannot find this task with id [" + taskid + "].");
            return;
        }

        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

        // 4、查询指定日期范围内的用户访问行为数据
        JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(sqlContext, taskParam);
        // 对用户访问行为数据做一个映射，将其映射为<sessionid,访问行为>的格式
        JavaPairRDD<String, Row> sessionid2actionRDD = PageFunctionMyTest.getSessionid2actionRDD(actionRDD);
        sessionid2actionRDD = sessionid2actionRDD.cache(); // 缓存一下
        // 拿到每个session的动作集合
        JavaPairRDD<String, Iterable<Row>> sessionid2actionsRDD = sessionid2actionRDD.groupByKey();
        sessionid2actionsRDD = sessionid2actionsRDD.cache();

        // 最核心的一步，每个session的单跳页面切片的生成，以及页面流的匹配，算法：key为 previousPageId + "_" + pageid
        JavaPairRDD<String, Integer> pageSplitRDD = PageFunctionMyTest.generateAndMatchPageSplit(sc, sessionid2actionsRDD, taskParam);
        Map<String, Object> pageSplitPvMap = pageSplitRDD.countByKey(); // key为 previousPageId + "_" + pageid

        // 拿到的是每个session的访问指定起始页的累计数
        long startPagePv = PageFunctionMyTest.getStartPagePv(taskParam, sessionid2actionsRDD);

        // 计算目标页面流的各个页面切片的转化率
        Map<String, Double> convertRateMap = PageFunctionMyTest.computePageSplitConvertRate(
                taskParam, pageSplitPvMap, startPagePv);

        // 持久化页面切片转化率
        PageFunctionMyTest.persistConvertRate(taskid, convertRateMap);
    }


}
