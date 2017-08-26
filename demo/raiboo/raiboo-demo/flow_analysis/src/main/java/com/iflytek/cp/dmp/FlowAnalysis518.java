package com.iflytek.cp.dmp;
//package com.iflytek.cp.dmp;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by taochen4 on 2017/05/11.
 */
public class FlowAnalysis518 {

    private MysqlDataSource dataSource = new MysqlDataSource();
    private QueryRunner run = null;
    private String querySql = "";
    private boolean inited = false;
    private ResultSetHandler<StatisticRes> queryHandler;

    private void init() {
        if (inited == true) {
            return;
        }

        dataSource.setURL("jdbc:mysql://172.16.59.13:10065/ifly_cpcc_bi_raiboo");
        dataSource.setUser("raiboo_view");
        dataSource.setPassword("HJB1RfkOn9oLj41J");
        run = new QueryRunner(dataSource);
        querySql = "select * from raiboo_demo_analysis_daily where timestamp=?";
        queryHandler =  new ResultSetHandler<StatisticRes>() {
            StatisticRes ret = new StatisticRes();
            public StatisticRes handle(ResultSet rs) throws SQLException {
                while (rs.next()) {
                    StatisticRes statisticRes = new StatisticRes();
                    if (rs.getString(3).equals("28@2@0@38")) {   // 入口
                        statisticRes.enter_count = rs.getInt(4);
                    } else if (rs.getString(3).equals("28@6@0@40")) {
                        statisticRes.exhibition_count = rs.getInt(4);
                    } else if (rs.getString(3).equals("prize")) {
                        statisticRes.play_count = rs.getInt(4);
                    }
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
        return dateToStamp(start.toString());
    }

    public List<StatisticRes> getStatisticResList(Long now) throws ParseException, SQLException {
        List<Long> timeList = new ArrayList<Long>();
        Long today_start = getTodayStart();
        Long interval = (now - today_start)/10;
        for (int i = 1; i <= 10; ++i) {
            timeList.add(today_start + i * interval);
        }
//        for (int i = 0; i < 10; ++i) {
//            System.out.println(stampToDateHM(timeList.get(i)));
//            System.out.println(stampToDate(timeList.get(i)));
//        }
        List<StatisticRes> ret = new ArrayList<StatisticRes>();
        for (Long timestamp : timeList) {
            StatisticRes query = run.query(querySql, queryHandler, timestamp);
            query.datatime = stampToDateHM(timestamp);
            ret.add(query);
        }
        return ret;
    }

    public static void main(String[] args) throws ParseException, SQLException {
//        System.out.println( stampToDate(new Date().getTime()/1000) );
//        System.out.println(new Date());
        FlowAnalysis518 flowAnalysis518 = new FlowAnalysis518();
        flowAnalysis518.getStatisticResList(new Date().getTime()/1000);
    }
}
