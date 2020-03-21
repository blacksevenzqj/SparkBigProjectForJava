package com.ibeifeng.sparkproject.spark.page;

import com.alibaba.fastjson.JSONObject;
import com.ibeifeng.sparkproject.constant.Constants;
import com.ibeifeng.sparkproject.dao.IPageSplitConvertRateDAO;
import com.ibeifeng.sparkproject.dao.factory.DAOFactory;
import com.ibeifeng.sparkproject.domain.PageSplitConvertRate;
import com.ibeifeng.sparkproject.util.DateUtils;
import com.ibeifeng.sparkproject.util.NumberUtils;
import com.ibeifeng.sparkproject.util.ParamUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.util.*;

public class PageFunctionMyTest {

    public static JavaPairRDD<String, Row> getSessionid2actionRDD(JavaRDD<Row> actionRDD) {
        return actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                String sessionid = row.getString(2);
                return new Tuple2<String, Row>(sessionid, row);
            }
        });
    }


    public static JavaPairRDD<String, Integer> generateAndMatchPageSplit(
            JavaSparkContext sc,
            JavaPairRDD<String, Iterable<Row>> sessionid2actionsRDD,
            JSONObject taskParam) {
        String targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);
        final Broadcast<String> targetPageFlowBroadcast = sc.broadcast(targetPageFlow);

        return sessionid2actionsRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, String, Integer>() {
            @Override
            public Iterable<Tuple2<String, Integer>> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                List<Tuple2<String, Integer>> resultList = new ArrayList<Tuple2<String, Integer>>();

                String[] targetPages = targetPageFlowBroadcast.value().split(",");

                List<Row> rowList = new ArrayList<Row>();
                Iterator<Row> iterator = tuple._2.iterator();
                while(iterator.hasNext()){
                    rowList.add(iterator.next());
                }

                Collections.sort(rowList, new Comparator<Row>() {
                    @Override
                    public int compare(Row r1, Row r2) {
                        String time1 = r1.getString(4);
                        String time2 = r2.getString(4);
                        Date date1 = DateUtils.parseTime(time1);
                        Date date2 = DateUtils.parseTime(time2);
                        return (int)(date1.getTime() - date2.getTime());
                    }
                });

                // 页面切片的生成，以及页面流的匹配
                Long previousPageId = null;
                for(Row row : rowList){
                    long pageid = row.getLong(3);
                    if(previousPageId == null){
                        previousPageId = pageid;
                        continue;
                    }
                    String pageSplit = previousPageId + "_" + pageid;
                    // 对这个切片判断一下，是否在用户指定的页面流中
                    for(int i = 1; i < targetPages.length; i++) {
                        // 比如说，用户指定的页面流是3,2,5,8,1
                        // 遍历的时候，从索引1开始，就是从第二个页面开始
                        // 3_2
                        String targetPageSplit = targetPages[i - 1] + "_" + targetPages[i];

                        if(pageSplit.equals(targetPageSplit)) {
                            resultList.add(new Tuple2<String, Integer>(pageSplit, 1));
                            break;
                        }
                    }
                    previousPageId = pageid;
                }
                return resultList;
            }
        });
    }


    public static long getStartPagePv(JSONObject taskParam, JavaPairRDD<String, Iterable<Row>> sessionid2actionsRDD) {
        String targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);
        final long startPageId = Long.valueOf(targetPageFlow.split(",")[0]);
        JavaRDD<Long> startPage = sessionid2actionsRDD.flatMap(new FlatMapFunction<Tuple2<String, Iterable<Row>>, Long>() {
            @Override
            public Iterable<Long> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                List<Long> resultList = new ArrayList();
                Iterator<Row> iterator = tuple._2.iterator();
                while (iterator.hasNext()){
                    Row row = iterator.next();
                    long pageid = row.getLong(3);
                    if(pageid == startPageId) {
                        resultList.add(pageid);
                    }
                }
                return resultList;
            }
        });
        return startPage.count();
    }


    public static Map<String, Double> computePageSplitConvertRate(JSONObject taskParam, Map<String, Object> pageSplitPvMap,
                                                                  long startPagePv) {
        Map<String, Double> convertRateMap = new HashMap<String, Double>();
        String[] targetPages = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW).split(",");

        Long previousPagePv = 0L;
        for(int i = 1; i < targetPages.length; i++){
            String targetPage = targetPages[i-1] + "_" + targetPages[i];
            Object temp = pageSplitPvMap.get(targetPage);
            if(temp == null){
                continue;
            }
            Long targetPagePV = Long.valueOf(String.valueOf(temp));

            double convertRate = 0.0;

            if(i == 1) {
                convertRate = NumberUtils.formatDouble(
                        (double)targetPagePV / (double)startPagePv, 2);
            } else {
                convertRate = NumberUtils.formatDouble(
                        (double)targetPagePV / (double)previousPagePv, 2);
            }
            convertRateMap.put(targetPage, convertRate);
            previousPagePv = targetPagePV;
        }
        return convertRateMap;
    }


    public static void persistConvertRate(long taskid, Map<String, Double> convertRateMap) {
        StringBuffer buffer = new StringBuffer("");

        for(Map.Entry<String, Double> convertRateEntry : convertRateMap.entrySet()) {
            String pageSplit = convertRateEntry.getKey();
            double convertRate = convertRateEntry.getValue();
            buffer.append(pageSplit + "=" + convertRate + "|");
        }

        String convertRate = buffer.toString();
        convertRate = convertRate.substring(0, convertRate.length() - 1);

        PageSplitConvertRate pageSplitConvertRate = new PageSplitConvertRate();
        pageSplitConvertRate.setTaskid(taskid);
        pageSplitConvertRate.setConvertRate(convertRate);

        IPageSplitConvertRateDAO pageSplitConvertRateDAO = DAOFactory.getPageSplitConvertRateDAO();
        pageSplitConvertRateDAO.insert(pageSplitConvertRate);
    }
}
