package com.iflytek.cp.dmp;
//package com.iflytek.cp.dmp;

import com.google.gson.Gson;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by taochen4 on 2016/11/22.
 */
public class FlowAnalysisStop {

    private static class Market implements Cloneable {
        public String id = new String();
        public String name = new String();
        public String longtitude = new String();
        public String latitude = new String();
        public String location = new String();
        public String province = new String();
        public String city = new String();

        @Override
        protected Market clone() throws CloneNotSupportedException {
            Market m = new Market();
            m.id = this.id;
            m.name = this.name;
            m.longtitude = this.longtitude;
            m.latitude = this.latitude;
            m.location = this.location;
            m.province = this.province;
            m.city = this.city;
            return m;
        }
    }

    public static class FlowResult implements Cloneable,Comparable<FlowResult> {
        public String  marketID = "";
        public Market market = new Market();
        public Long marketFlow = 0l;
        public Long rackFlow = 0l;
        public Long interactFlow = 0l;
        public Long messageFlow = 0l;
        public Long testFlow = 0l;
        public Long timestamp = 0l;

        public void clear() {
            marketID = "";
            market = new Market();
            marketFlow = 0l;
            rackFlow = 0l;
            interactFlow = 0l;
            messageFlow = 0l;
            timestamp = 0l;
            testFlow = 0l;
        }

        public void update(FlowResult f) {
            if (f.marketFlow != 0) {
                this.marketFlow = f.marketFlow;
            }

            if (f.rackFlow != 0) {
                this.rackFlow = f.rackFlow;
            }

            if (f.interactFlow != 0) {
                this.interactFlow = f.interactFlow;
            }

            if (f.messageFlow != 0) {
                this.messageFlow = f.messageFlow;
            }

            if (f.testFlow != 0) {
                this.testFlow = f.testFlow;
            }
        }

        public void add(FlowResult r) {
            this.marketFlow += r.marketFlow;
            this.testFlow += r.testFlow;
            this.interactFlow += r.interactFlow;
            this.messageFlow += r.messageFlow;
            this.rackFlow += r.rackFlow;
        }

        protected FlowResult clone() throws CloneNotSupportedException {
            FlowResult cl = new FlowResult();
            cl.update(this);
            cl.marketID = this.marketID;
            cl.market = market!=null?market:new Market();
            cl.timestamp = this.timestamp;
            return cl;
        }

        public String toString() {
            return " market id:" + marketID +
                    " market name:" + market.location +
                    ", market flow:" + marketFlow +
                    ", rack flow:" + rackFlow +
                    ", interact flow:" + interactFlow +
                    ", message flow:" + messageFlow +
                    ", test flow:" + testFlow +
                    ", timestamp[" + timestamp + "]:" + (FlowAnalysisStop.stampToDate(timestamp>1480629600000l?timestamp/1000:timestamp));
        }

        public int compareTo(FlowResult o) {
            return timestamp >= o.timestamp ? 0 : 1;
        }
    }

    private class SQLResult implements Cloneable{
        public String id = "";
        public Long flowCount = 0l;
        public Long timestamp = 0l;
        public String type = "";

        @Override
        protected SQLResult clone() throws CloneNotSupportedException {
            SQLResult s = new SQLResult();
            s.id = this.id;
            s.flowCount = this.flowCount;
            s.timestamp = this.timestamp;
            s.type = this.type;
            return s;
        }
    }

    private class ApiResult{
        String market_name = "";
        String latitude = "";
        String longtitude = "";
        HashMap<String, String> flow = new HashMap<String, String>();
    }

    private MysqlDataSource dataSource = new MysqlDataSource();
    private QueryRunner run = null;
    private boolean inited = false;
    private String probeSql = "";
    private String questionnairSql = "";
    private String msgSql = "";
    private String marketSql = "";
    private String probeDailySql="";
    private Map<String, Market> markets;
    private ResultSetHandler<List<SQLResult>> getProbeHandler;
    private ResultSetHandler<List<SQLResult>> getQuestionnairHandler;
    private ResultSetHandler<List<SQLResult>> getMsgHandler;
    private ResultSetHandler<Map<String, Market>> getMarketHandler;
    private Gson gson = new Gson();

    public static long dateToStamp(String s) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = simpleDateFormat.parse(s);
        long ts = date.getTime()/1000;
        return ts;
    }

    public static String stampToDate(Long lt){
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date(lt*1000);
        res = simpleDateFormat.format(date);
        return res;
    }

    public Long getTodayStart() throws ParseException {
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.MONTH, 0);
        calendar.set(Calendar.DAY_OF_MONTH, 10);
        StringBuffer start = new StringBuffer();
        start.append(calendar.get(Calendar.YEAR));
        start.append("-");
        start.append(calendar.get(Calendar.MONTH) + 1);
        start.append("-");
        start.append(calendar.get(Calendar.DAY_OF_MONTH));
        start.append(" 00:00:00");
        System.out.println(start);
        return dateToStamp(start.toString());
    }

    public Long getTodayEnd() throws ParseException {
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.MONTH, 0);
        calendar.set(Calendar.DAY_OF_MONTH, 10);
        StringBuffer end = new StringBuffer();
        end.append(calendar.get(Calendar.YEAR));
        end.append("-");
        end.append(calendar.get(Calendar.MONTH) + 1 );
        end.append("-");
        end.append(calendar.get(Calendar.DAY_OF_MONTH));
        end.append(" 23:59:59");
        return dateToStamp(end.toString());
    }

    private Long getWeekStart() throws ParseException {
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.MONTH, 0);
        calendar.set(Calendar.DAY_OF_MONTH, 10);
        calendar.add(Calendar.DAY_OF_MONTH, -6);
        StringBuffer start = new StringBuffer();
        start.append(calendar.get(Calendar.YEAR));
        start.append("-");
        start.append(calendar.get(Calendar.MONTH) + 1);
        start.append("-");
        start.append(calendar.get(Calendar.DAY_OF_MONTH));
        start.append(" 00:00:00");
        return dateToStamp(start.toString());
    }

    private Long getMonthStart() throws ParseException {
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.MONTH, 0);
        calendar.set(Calendar.DAY_OF_MONTH, 10);
        calendar.add(Calendar.DAY_OF_MONTH, -29);
        StringBuffer start = new StringBuffer();
        start.append(calendar.get(Calendar.YEAR));
        start.append("-");
        start.append(calendar.get(Calendar.MONTH) + 1);
        start.append("-");
        start.append(calendar.get(Calendar.DAY_OF_MONTH));
        start.append(" 00:00:00");
        return dateToStamp(start.toString());
    }

    private void init() {
        if (inited == true) {
            return;
        }

        dataSource.setURL("jdbc:mysql://172.16.59.13:10065/ifly_cpcc_bi_raiboo");
        dataSource.setUser("raiboo_view");
        dataSource.setPassword("HJB1RfkOn9oLj41J");
        run = new QueryRunner(dataSource);
        markets = new HashMap<String, Market>();

        probeDailySql = "select" +
                "  market_id," +
                "  timestamp," +
                "  active_user," +
                "  probe_type" +
                " from " +
                " raiboo_analysis_realtime_traffic_daily " +
                "where `timestamp` >=? and `timestamp` <=? " +
                "and probe_id='ALL' and brand_id='ALL' and market_id!='ALL' and probe_type!='ALL'";

        probeSql = "select" +
                "  market_id," +
                "  timestamp," +
                "  active_user," +
                "  probe_type" +
                " from " +
                " raiboo_analysis_realtime_traffic_hourly " +
                "where `timestamp` >=? and `timestamp` <=? " +
                "and probe_id='ALL' and brand_id='ALL' and market_id!='ALL' and probe_type!='ALL'";

        questionnairSql = "select" +
                "  market_id," +
                "  timestamp," +
                "  sess_times" +
                " from " +
                " raiboo_analysis_realtime_questionnaire_hourly" +
                " where `timestamp` >=? and `timestamp` <=? " +
                " and brand_id=0 and market_id!='ALL'";

        msgSql = "select " +
                "  market_id," +
                "  datetime," +
                "  count(*) " +
                "from" +
                "(" +
                "  SELECT" +
                "  UNIX_TIMESTAMP( FROM_UNIXTIME(`timestamp`, '%Y-%m%-%d %H:00:00')) as datetime," +
                "  market_id as market_id" +
                "  FROM  raiboo_promotion_message_data where phonenumber !=\"\" and phonenumber != 0 and message_id!=\"-1\" " +
                ") a  where datetime >=? and datetime <=? group by market_id, datetime ";

        System.out.println(msgSql);

        marketSql = "select market_id,market_name,longtitude,latitude,location,province,city from raiboo_market where is_act=1";

        getProbeHandler = new ResultSetHandler<List<SQLResult>>() {
            public List<SQLResult> handle(ResultSet rs) throws SQLException {
                List<SQLResult> ret = new ArrayList<SQLResult>();
                SQLResult sqlResult = new SQLResult();
                while (rs.next()) {
                    sqlResult.id = (String)rs.getObject(1);
                    sqlResult.timestamp = (Long) rs.getObject(2);
                    sqlResult.flowCount = (Long) rs.getObject(3);
                    sqlResult.type = (String)rs.getObject(4);
                    try {
                        ret.add(sqlResult.clone());
                    } catch (CloneNotSupportedException e) {
                        e.printStackTrace();
                    }
                }
                return ret;
            }
        };

        getQuestionnairHandler = new ResultSetHandler<List<SQLResult>>() {
            private List<SQLResult> ret = new ArrayList<SQLResult>();
            public List<SQLResult> handle(ResultSet rs) throws SQLException {
                ret = new ArrayList<SQLResult>();
                SQLResult sqlResult = new SQLResult();
                while (rs.next()) {
                    sqlResult.id = (String)rs.getObject(1);
                    sqlResult.timestamp = (Long) rs.getObject(2);
                    sqlResult.flowCount = (Long) rs.getObject(3);
                    try {
                        ret.add(sqlResult.clone());
                    } catch (CloneNotSupportedException e) {
                        e.printStackTrace();
                    }
                }
                return ret;
            }
        };

        getMsgHandler = new ResultSetHandler<List<SQLResult>>() {
            private List<SQLResult> ret = new ArrayList<SQLResult>();
            public List<SQLResult> handle(ResultSet rs) throws SQLException {
                ret = new ArrayList<SQLResult>();
                SQLResult sqlResult = new SQLResult();
                while (rs.next()) {
                    sqlResult.id = rs.getObject(1).toString();
                    sqlResult.timestamp = (Long) rs.getObject(2);
                    sqlResult.flowCount = (Long) rs.getObject(3);
                    try {
                        ret.add(sqlResult.clone());
                    } catch (CloneNotSupportedException e) {
                        e.printStackTrace();
                    }
                }
                return ret;
            }
        };

        getMarketHandler = new ResultSetHandler<Map<String, Market>>() {
            private Map<String, Market> ret = new HashMap<String, Market>();
            public Map<String, Market> handle(ResultSet rs) throws SQLException {
                Market m = new Market();
                // "select marketid,market_name,longtitude,latitude,location,province,city from raiboo_market";
                while(rs.next()) {
                    m.id = rs.getObject(1).toString();
                    m.name = (String) rs.getObject(2);
                    m.longtitude = rs.getObject(3).toString();
                    m.latitude =  rs.getObject(4).toString();
                    m.location = (String) rs.getObject(5);
                    m.province = (String) rs.getObject(6);
                    m.city = (String) rs.getObject(7);
                    try {
                        ret.put(m.id, m.clone());
                    } catch (CloneNotSupportedException e) {
                        e.printStackTrace();
                    }
                }
                return ret;
            }
        };
        inited = true;
    }

    public Map<String, Market> getMarketWithID() throws SQLException, ParseException {
        init();
        markets = run.query(marketSql, getMarketHandler);
        return markets;
    }

    // 返回值以market id + timestamp 为维度
    private synchronized List<FlowResult> getAnalysisByHour(Long start, Long end) throws SQLException, ParseException, CloneNotSupportedException {
        init();
        Map<String, FlowResult> ret = new HashMap<String, FlowResult>();
        FlowResult fr = new FlowResult();
        // 获取探针的数据
        List<SQLResult> rs = run.query(probeSql, getProbeHandler, start.toString(), end.toString());
        for (SQLResult r : rs) {
            fr.clear();
            if (r.type.equals("2")) {   // 商超出入口
                fr.marketFlow = r.flowCount;
            } else if (r.type.equals("6")) {   // 货架
                fr.rackFlow = r.flowCount;
            } else if (r.type.equals("0")) {
                fr.testFlow = r.flowCount;
            }
            fr.marketID = r.id;
            fr.timestamp = r.timestamp;
//            if (fr.marketID.equals("6")) {
//                System.out.println("probe:" + fr);
//            }

            if (ret.containsKey(r.id + r.timestamp.toString())) {
                ret.get(r.id + r.timestamp.toString()).update(fr);
            } else {
                ret.put(r.id + r.timestamp.toString(), fr.clone());
            }
//            System.out.println(fr);
        }

        rs.clear();
        rs = run.query(questionnairSql, getQuestionnairHandler, start.toString(), end.toString());
        for (SQLResult r : rs) {
            fr.clear();
            fr.marketID = r.id;
            fr.interactFlow = r.flowCount;
            fr.timestamp = r.timestamp;
//            System.out.println("questionnair:" + fr);
            if (ret.containsKey(r.id + r.timestamp.toString())) {
                ret.get(r.id + r.timestamp.toString()).update(fr);
            } else {
                ret.put(r.id + r.timestamp.toString(), fr.clone());
            }
//            System.out.println(fr);
        }

        rs.clear();
        rs = run.query(msgSql, getMsgHandler, start.toString(), end.toString());
        for (SQLResult r : rs) {
            fr.clear();
            fr.marketID = r.id;
            fr.messageFlow = r.flowCount;
            fr.timestamp = r.timestamp;
//            System.out.println("msg:" + fr);
            if (ret.containsKey(r.id + r.timestamp.toString())) {
                ret.get(r.id + r.timestamp.toString()).update(fr);
            } else {
                ret.put(r.id + r.timestamp.toString(), fr.clone());
            }
//            System.out.println(fr);
        }

        List<FlowResult> result = new ArrayList<FlowResult>();
        for (Map.Entry<String, FlowResult> entry : ret.entrySet()) {
            Market m = markets.get(entry.getValue().marketID);
            if ( m == null) {
                markets = getMarketWithID();
            }
            m = markets.get(entry.getValue().marketID);
            if (m != null) {
                entry.getValue().market = m == null ? new Market() : m;
                result.add(entry.getValue());
            }
//            System.out.println("== [ key:" + entry.getKey() + entry.getValue() + " ] ==");
        }
//        Collections.sort(result);
//         System.out.println(ret.toString());
        return result;
    }

    private List<FlowResult> getAnalysisByDay(List<FlowResult> flowResultList, Long start, Long end) throws CloneNotSupportedException, SQLException, ParseException {
        Calendar calendar = Calendar.getInstance();
        Map<String, FlowResult> ret = new HashMap<String, FlowResult>();
        for(FlowResult r : flowResultList) {
            calendar.setTimeInMillis(r.timestamp*1000); // *1000是因为java精确到毫秒
            calendar.set(Calendar.HOUR_OF_DAY, 0);
            calendar.set(Calendar.MINUTE, 0);
            calendar.set(Calendar.SECOND, 0);
            r.timestamp = calendar.getTime().getTime();
            r.timestamp /=1000;
            if (ret.containsKey(r.marketID + r.timestamp)) {
                ret.get(r.marketID + r.timestamp).add(r);
//                System.out.println(r);
            } else {
                ret.put(r.marketID + r.timestamp , r.clone());
//                System.out.println("1:" + r.marketID + r.timestamp +  r);
            }
        }

        // 清空商超入口和货架
        for (Map.Entry<String, FlowResult> entry : ret.entrySet()) {
            entry.getValue().marketFlow = 0l;
            entry.getValue().rackFlow = 0l;
//            System.out.println("3:" + entry.getKey());
        }

        // 更新探针Daily表数据
        FlowResult fr = new FlowResult();
        List<SQLResult> rs = run.query(probeDailySql, getProbeHandler, start, end);
        for (SQLResult r : rs) {
            fr.clear();
            if (r.type.equals("2")) {   // 商超出入口
                fr.marketFlow = r.flowCount;
            } else if (r.type.equals("6")) {   // 货架
                fr.rackFlow = r.flowCount;
            } else if (r.type.equals("0")) {
                fr.testFlow = r.flowCount;
            }
            fr.marketID = r.id;
            fr.timestamp = r.timestamp;
//            System.out.println("msg:" + fr);
            if (ret.containsKey(r.id + r.timestamp.toString())) {
                ret.get(r.id + r.timestamp.toString()).update(fr);
//                System.out.println("update:" + fr.marketID + fr.timestamp + fr);
            } else {
                ret.put(r.id + r.timestamp.toString(), fr.clone());
//                System.out.println("add:" + fr.marketID + fr.timestamp + fr);
            }

        }

//        System.out.println(ret);
        List<FlowResult> result = new ArrayList<FlowResult>();
        for (Map.Entry<String, FlowResult> entry : ret.entrySet()) {
            Market m = markets.get(entry.getValue().marketID);
            if ( m == null) {
                markets = getMarketWithID();
            }
            m = markets.get(entry.getValue().marketID);
            if (m != null) {
                entry.getValue().market = m == null ? new Market() : m;
                result.add(entry.getValue());
            }
//            System.out.println("== [ key:" + entry.getKey() + entry.getValue() + " ] ==");
        }
//        Collections.sort(result);
//        System.out.println(result.size());
        return result;
    }

    public synchronized String getRealTimeFlow(String type, String granularity, String marketID) throws ParseException, CloneNotSupportedException, SQLException {
        List<FlowResult> result = null;
        if (type.length() == 0 && granularity.length() == 0) {
            result = getDayAnalysisByDay();
            List<ApiResult> ret = new ArrayList<ApiResult>();
            for (FlowResult r : result) {
                ApiResult apiResult = new ApiResult();
                apiResult.market_name = r.market.name;
                apiResult.longtitude = r.market.longtitude;
                apiResult.latitude = r.market.latitude;
                apiResult.flow.put("market_id", r.marketID);
                apiResult.flow.put("商超客流", r.marketFlow.toString());
                apiResult.flow.put("货架客流", r.rackFlow.toString());
                apiResult.flow.put("互动客流", r.interactFlow.toString());
                apiResult.flow.put("推送活动短信", r.messageFlow.toString());
                ret.add(apiResult);
            }
//            System.out.println(ret.size());
//            System.out.println("flowAnaylsis type=&granularity=&maketID= size:" + ret.size() + "\n" + gson.toJson(ret));
            return gson.toJson(ret);
        }

        List<HashMap<Long, Long>> ret = new ArrayList<HashMap<Long, Long>>();
        HashMap<Long, Long> hm = new HashMap<Long, Long>();
        ret.add(hm);
        hm = new HashMap<Long, Long>();
        ret.add(hm);
        hm = new HashMap<Long, Long>();
        ret.add(hm);
        hm = new HashMap<Long, Long>();
        ret.add(hm);

        if (type.equals("day")) {
            result = getDayAnalysisByHour();
        } else if (type.equals("week") && granularity.equals("hour")) {
            result = getWeekAnalysisByHour();
        } else if (type.equals("week") && granularity.equals("day")) {
            result = getWeekAnalysisByDay();
        } else if (type.equals("month")) {
            result = getMonthAnalysisByDay();
        } else {
            return gson.toJson(ret);
        }

        if (marketID.equals("ALL") || marketID.length() == 0) {
            for (FlowResult fr : result) {
//                System.out.println(fr.marketID + ":" + marketID);
//                System.out.println(fr);
                ret.get(0).put(fr.timestamp, (ret.get(0).containsKey(fr.timestamp)?ret.get(0).get(fr.timestamp):0) + fr.marketFlow);
                ret.get(1).put(fr.timestamp, (ret.get(1).containsKey(fr.timestamp)?ret.get(1).get(fr.timestamp):0) + fr.rackFlow);
                ret.get(2).put(fr.timestamp, (ret.get(2).containsKey(fr.timestamp)?ret.get(2).get(fr.timestamp):0) + fr.interactFlow);
                ret.get(3).put(fr.timestamp, (ret.get(3).containsKey(fr.timestamp)?ret.get(3).get(fr.timestamp):0) + fr.messageFlow);
//                System.out.println(ret.get(0));
            }
        } else {
            for (FlowResult fr : result) {
//                System.out.println(fr.marketID + ":" + marketID);
                if (fr.marketID.equals(marketID)) {
//                    System.out.println(fr.marketID + ":" + marketID);
//                    System.out.println(fr);
                    ret.get(0).put(fr.timestamp, (ret.get(0).containsKey(fr.timestamp)?ret.get(0).get(fr.timestamp):0) + fr.marketFlow);
                    ret.get(1).put(fr.timestamp, (ret.get(1).containsKey(fr.timestamp)?ret.get(1).get(fr.timestamp):0) + fr.rackFlow);
                    ret.get(2).put(fr.timestamp, (ret.get(2).containsKey(fr.timestamp)?ret.get(2).get(fr.timestamp):0) + fr.interactFlow);
                    ret.get(3).put(fr.timestamp, (ret.get(3).containsKey(fr.timestamp)?ret.get(3).get(fr.timestamp):0) + fr.messageFlow);
                }
            }
        }
        Long start = 0l;
        Long end = getTodayEnd();
        Long fre = 0l;
        if (type.equals("day")) {
            start = getTodayStart();
        } else if (type.equals("week")) {
            start = getWeekStart();
        } else if (type.equals("month")) {
            start = getMonthStart();
        }
        if (type.equals("day") || (type.equals("week") && granularity.equals("hour"))) {
            fre = 3600l;
        } else {
            fre = 86400l;
        }
        for (Long s = start; s < end; s += fre) {
            if (!ret.get(0).containsKey(s)) {
                ret.get(0).put(s, 0l);
                ret.get(1).put(s, 0l);
                ret.get(2).put(s, 0l);
                ret.get(3).put(s, 0l);
            }
        }
//        System.out.println(gson.toJson(ret));
        return gson.toJson(ret);
    }

    private List<FlowResult> getFlowAnalysisByDay() {

        return null;
    }

    private List<FlowResult> getDayAnalysisByHour() throws SQLException, ParseException, CloneNotSupportedException {
        return getAnalysisByHour(getTodayStart(), getTodayEnd());
    }

    private List<FlowResult> getWeekAnalysisByHour() throws ParseException, SQLException, CloneNotSupportedException {
        return getAnalysisByHour(getWeekStart(), getTodayEnd());
    }

    private List<FlowResult> getDayAnalysisByDay() throws SQLException, ParseException, CloneNotSupportedException {
        List<FlowResult> flowResultList = getAnalysisByHour(getTodayStart(), getTodayEnd());
        return getAnalysisByDay(flowResultList, getTodayStart(), getTodayEnd());
    }

    private List<FlowResult> getWeekAnalysisByDay() throws ParseException, SQLException, CloneNotSupportedException {
        List<FlowResult> flowResultList = getAnalysisByHour(getWeekStart(), getTodayEnd());
        return getAnalysisByDay(flowResultList, getWeekStart(), getTodayEnd());
    }

    private List<FlowResult> getMonthAnalysisByDay() throws ParseException, SQLException, CloneNotSupportedException {
        List<FlowResult> flowResultList = getAnalysisByHour(getMonthStart(), getTodayEnd());
        return getAnalysisByDay(flowResultList, getMonthStart(), getTodayEnd());
    }

    public static void main(String[] args) throws SQLException, ParseException, CloneNotSupportedException {
        FlowAnalysisStop flowAnalysis = new FlowAnalysisStop();
        flowAnalysis.init();
        System.out.println(flowAnalysis.getRealTimeFlow("day","",""));
//        StringBuffer sb = new StringBuffer();
//        TreeSet<String> tmpSet = new TreeSet<String>();
//        tmpSet.add("1");
//        tmpSet.add("2");
//        tmpSet.add("3");
//        tmpSet.add("16");
//        tmpSet.add("2");
//        tmpSet.add("11");
//        sb.append(tmpSet);
//        System.out.print(sb);
//        FlowAnalysisStop flowAnalysis = new FlowAnalysisStop();
//        flowAnalysis.getTodayStart();

    }
}
