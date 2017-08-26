package dmp;
//package com.iflytek.cp.dmp;

import com.google.gson.Gson;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * Created by taochen4 on 2017/05/11.
 */
public class FlowAnalysis {

    private MysqlDataSource dataSource = new MysqlDataSource();
    private QueryRunner run = null;
    private String entryQuerySql = "";
    private String exhibitionQuerySql = "";
    private String playQuerySql = "";
    private String insertSql = "";
    private boolean inited = false;
    Long today_start = -1l;
    private ResultSetHandler<Integer> queryHandler;

    public void init() {
        if (inited == true) {
            return;
        }
        dataSource.setURL("jdbc:mysql://172.16.59.13:10065/ifly_cpcc_bi_raiboo");
//        dataSource.setUser("raiboo_view");
//        dataSource.setPassword("HJB1RfkOn9oLj41J");
        dataSource.setUser("raiboo_biz");
        dataSource.setPassword("AxiaxxqEun8ZnROX");
        run = new QueryRunner(dataSource);
        entryQuerySql = "select active_users from raiboo_demo_analysis_daily where probe_id='42' " +
                "and `timestamp`>=? and `timestamp`<=? order by timestamp desc limit 1";
        exhibitionQuerySql = "select active_users from raiboo_demo_analysis_daily where probe_id='44' " +
                "and `timestamp`>=? and `timestamp`<=? order by timestamp desc limit 1";
        playQuerySql = "select active_users from raiboo_demo_analysis_daily where probe_id='prize' " +
                "and `timestamp`>=? and `timestamp`<=? order by timestamp desc limit 1";

        insertSql = "insert into raiboo_demo_analysis_daily(`timestamp`, probe_id, active_users) values( ?, ?, ?)";
        queryHandler =  new ResultSetHandler<Integer>() {
            public Integer handle(ResultSet rs) throws SQLException {
                Integer ret = 0;
                while (rs.next()) {
                        ret = rs.getInt(1);
                }
                return ret;
            }
        };
    }

    public static class StatisticRes {
        public String datatime;
        public int enter_count;
        public int exhibition_count;
        public int play_count;

        /*
         *  28@2@0@38  入口
         *  28@6@0@40  展台
         *  prize      互动
         */
        public StatisticRes(){
            datatime = "";
            enter_count = 0;
            exhibition_count = 0;
            play_count = 0;
        }
    }

    public static long dateToStamp(String s) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = simpleDateFormat.parse(s);
        long ts = date.getTime()/1000;
        return ts;
    }

//    public static long getMinuteStamp(Long s) throws ParseException {
//        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
//        Date date = simpleDateFormat.parse(simpleDateFormat.format(s));
//        long ts = date.getTime()/1000;
//        return ts;
//    }

    public String stampToDateHM(Long lt){
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("HH:mm");
//        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date(lt*1000);
        res = simpleDateFormat.format(date);
        return res;
    }

    public String stampToDate(Long lt){
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date(lt*1000);
        res = simpleDateFormat.format(date);
        return res;
    }

    public Long getTodayStart() throws ParseException {
        Calendar calendar = Calendar.getInstance();
        StringBuffer start = new StringBuffer();
        start.append(calendar.get(Calendar.YEAR));
        start.append("-");
        start.append(calendar.get(Calendar.MONTH) + 1);
        start.append("-");
        start.append(calendar.get(Calendar.DAY_OF_MONTH));
        start.append(" 06:00:00");
//        Long ret = 1494972000l;  // 5月17号 6点
//        if (ret > dateToStamp(start.toString())) {
        return dateToStamp(start.toString());
//        } else {
//            return ret;
//        }
    }

    public void setInfo(Long datatime, String probe_id, int active_users) throws SQLException {
        run.update(insertSql,datatime, probe_id, active_users);
    }

    public List<StatisticRes> getStatisticResList(Long now) throws SQLException, ParseException {
        List<Long> timeList = new ArrayList<Long>();
        today_start = getTodayStart();
        if (now < today_start) {
            return null;
        }
        Long interval = (now - today_start)/10;
        for (int i = 9; i >= 0; --i) {
            timeList.add(now - i * interval);
        }
//        for (int i = 0; i < 10; ++i) {
//            System.out.println(stampToDateHM(timeList.get(i)) + ":" + timeList.get(i)/60*60);
////            System.out.println(stampToDate(timeList.get(i)));
//        }
        List<StatisticRes> ret = new ArrayList<StatisticRes>();
//        Gson gson = new Gson();
        for (Long timestamp : timeList) {
//            System.out.println(timestamp/60*60);
            StatisticRes query = new StatisticRes();

//            System.out.println("timestamp : " + stampToDateHM(timestamp));
//            System.out.println(entryQuerySql + " " + today_start + ":" +  timestamp/60*60);
            query.enter_count = run.query(entryQuerySql, queryHandler, today_start, timestamp/60*60);
//            System.out.println(query.enter_count);

//            System.out.println(exhibitionQuerySql + " " + today_start + ":" +  timestamp/60*60);
            query.exhibition_count = run.query(exhibitionQuerySql, queryHandler, today_start, timestamp/60*60);
//            System.out.println(query.exhibition_count);

//            System.out.println(playQuerySql + " " + today_start + ":" +  timestamp/60*60);
            query.play_count = run.query(playQuerySql, queryHandler, today_start, timestamp/60*60);
//            System.out.println(query.play_count );

            query.datatime = stampToDateHM(timestamp);
            ret.add(query);
        }
        return ret;
    }

    public static void main(String[] args) throws ParseException, SQLException {
//        System.out.println( stampToDate(new Date().getTime()/1000) );
//        System.out.println(new Date());
        FlowAnalysis flowAnalysis518 = new FlowAnalysis();
        flowAnalysis518.init();
//        flowAnalysis518.getStatisticResList(new Date().getTime()/1000);

        Long today = flowAnalysis518.getTodayStart();
//        for (int i=0; i<1000; ++i) {
//            String str = "";//"mysql -h 172.16.59.13:10065 -u raiboo_biz -p AxiaxxqEun8ZnROX -e ";
////            String datatime = Long.toString(today + i*60);
//            flowAnalysis518.setInfo(today + i*60, "28@2@0@38", i*20);
//            flowAnalysis518.setInfo(today + i*60, "28@6@0@40", i*10);
//            flowAnalysis518.setInfo(today + i*60, "prize", i*5);

////            String str1 = str + "insert into ifly_cpcc_bi_raiboo.raiboo_demo_analysis_daily(timestamp, probe_id, active_users) values( "+ datatime+", '28@2@0@38', " + i*20 + ")";
////            String str2 = str + "insert into ifly_cpcc_bi_raiboo.raiboo_demo_analysis_daily(timestamp, probe_id, active_users) values( "+ datatime+", '28@2@0@40', " + i*10 + ")";
////            String str3 = str + "insert into ifly_cpcc_bi_raiboo.raiboo_demo_analysis_daily(timestamp, probe_id, active_users) values( "+ datatime+", 'prize', " + i*5 + ")";
////            System.out.println(str1);
////            System.out.println(str2);
////            System.out.println(str3);
//        }
        Gson gson = new Gson();
//        System.out.println("stamp:" + flowAnalysis518.getMinuteStamp(new Date().getTime()/1000));
        System.out.println("stamp:" + new Date().getTime());
        System.out.println("stamp:" + new Date().getTime()/1000);
        System.out.println("stamp:" + new Date().getTime()/1000/60*60);
        System.out.println(flowAnalysis518.stampToDate(new Date().getTime()/1000/60*60));
        System.out.println(gson.toJson(flowAnalysis518.getStatisticResList(new Date().getTime()/1000/60*60)));
//        System.out.println(flowAnalysis518.dateToStamp("2017-05-17 06:00:00"));
    }
}
